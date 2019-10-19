from abc import ABCMeta, abstractmethod
import re
import os
import subprocess
import random
import time
import logging
import json

import requests

import gevent

import locust.runners
from locust import events

from zk_locust import split_zk_hosts, split_zk_host_port

_logger = logging.getLogger(__name__)

_zk_admin_scheme = os.getenv('ZK_ADMIN_SCHEME', 'http')
_zk_admin_port = int(os.getenv('ZK_ADMIN_PORT', '8080'))
_zk_admin_ping_timeout_ms = int(os.getenv('ZK_ADMIN_PING_TIMEOUT_MS', '100'))


def fetch_config():
    s = os.getenv('ZK_DISPATCH_CONFIG')
    if not s:
        return {}
    return json.loads(s)


_configs = fetch_config()

_config_sleep_ms = _configs.get('sleep_ms', 5000)
_config_sleep_after_disable_ms = _configs.get('sleep_after_disable_ms', 2000)
_config_sleep_after_enable_ms = _configs.get('sleep_after_enable_ms', 5000)

_config_program = os.getenv('ZK_DISPATCH_PROGRAM')

_initial_hatch_complete = False


def on_hatch_complete(user_count):
    global _initial_hatch_complete
    _initial_hatch_complete = True


events.hatch_complete += on_hatch_complete


def _compose_metrics_url(zk_host_port, command):
    host = split_zk_host_port(zk_host_port)[0]
    host_port = host + ':' + str(_zk_admin_port)
    url = _zk_admin_scheme + '://' + host_port + '/commands/' + command
    return url


_MEMBER_STATE_UNKNOWN, _MEMBER_STATE_FOLLOWER, _MEMBER_STATE_LEADER = [
    'unknown', 'follower', 'leader'
]
_MEMBER_STATES_UP = [_MEMBER_STATE_FOLLOWER, _MEMBER_STATE_LEADER]


class EnsembleMember(object):
    def __init__(self, host_and_port):
        self.host_and_port = host_and_port
        self.host, self.port = split_zk_host_port(host_and_port)
        self.http_session = requests.Session()
        self.ping_url = _compose_metrics_url(host_and_port, 'monitor')
        self.last_ping = None
        self.state = _MEMBER_STATE_UNKNOWN
        self.last_disabled = None

        self.http_session.get_adapter(self.ping_url).max_retries = 1

    def __str__(self):
        s = self.host_and_port + ' (state ' + self.state
        if self.last_ping:
            s += ' %3gs ago' % (time.time() - self.last_ping)
        s += ')'
        return s

    def is_up(self):
        return self.state in _MEMBER_STATES_UP

    def is_leader(self):
        return self.state == _MEMBER_STATE_LEADER

    def is_follower(self):
        return self.state == _MEMBER_STATE_FOLLOWER

    def ping(self):
        state = _MEMBER_STATE_UNKNOWN
        try:
            r = self.http_session.get(
                self.ping_url,
                allow_redirects=False,
                stream=False,
                timeout=_zk_admin_ping_timeout_ms / 1000)
            r.raise_for_status()
            v = json.loads(r.content)
            if v['error'] is None:
                server_state = v['server_state']
                if server_state == _MEMBER_STATE_FOLLOWER:
                    state = _MEMBER_STATE_FOLLOWER
                elif server_state == _MEMBER_STATE_LEADER:
                    state = _MEMBER_STATE_LEADER
        except requests.ConnectionError:
            pass
        except Exception:
            _logger.exception('Member status')
        finally:
            self.state = state
            self.last_ping = time.time()
        return self.state

    def note_disabled(self):
        self.last_disabled = time.time()

    def last_disabled_sort_key(self):
        return self.last_disabled or 0


class AbstractController(metaclass=ABCMeta):
    @abstractmethod
    def disable(self, member):
        pass

    @abstractmethod
    def enable(self, member):
        pass


class ShellScriptController(AbstractController):
    def _run_script(self, env_var, member):
        script = os.getenv(env_var)
        if not script:
            raise ValueError("Environment variable '%s' not set" % env_var)
        extra_env = {}
        for key in ['host_and_port', 'host', 'port', 'state']:
            extra_env['ZK_MEMBER_' + key.upper()] = str(getattr(member, key))
        _logger.debug("Invoking %s with %s" % (env_var, extra_env))
        env = dict(os.environ)
        env.update(extra_env)
        r = subprocess.call(script, shell=True, env=env)
        return r == 0

    def disable(self, member):
        return self._run_script('ZK_DISPATCH_DISABLE_SCRIPT', member)

    def enable(self, member):
        return self._run_script('ZK_DISPATCH_ENABLE_SCRIPT', member)


ACTION_DISABLE, ACTION_ENABLE = ['disable', 'enable']


class AbstractDispatcher(metaclass=ABCMeta):
    def __init__(self, *, controller=None):
        self.controller = controller or ShellScriptController()

    @abstractmethod
    def run(self, hosts_and_ports, quorum_size):
        pass

    def disable(self, member):
        if self.controller.disable(member):
            member.note_disabled()

    def enable(self, member):
        self.controller.enable(member)

    def sleep_ms(self, ms, cause=None):
        msg = 'Sleeping %dms' % ms
        if cause:
            msg += ' ' + cause
        _logger.debug(msg)
        gevent.sleep(ms / 1000)

    def sleep_after(self, action):
        if action == ACTION_DISABLE:
            ms = _config_sleep_after_disable_ms
            cause = 'after action ' + action
        elif action == ACTION_ENABLE:
            ms = _config_sleep_after_enable_ms
            cause = 'after action ' + action
        else:
            ms = _config_sleep_ms
            cause = None
        self.sleep_ms(ms, cause)

    def wait_initial_hatch_complete(self, sleep_ms=250):
        if _initial_hatch_complete:
            return

        _logger.debug('Polling for initial hatch complete; %dms', sleep_ms)
        while not _initial_hatch_complete:
            gevent.sleep(sleep_ms / 1000)
        _logger.debug('Initial hatch complete')

    def ping_ensemble(self, members):
        ups = []
        downs = []
        mark = time.time()
        for member in members:
            member.ping()
            if member.is_up():
                ups.append(member)
            else:
                downs.append(member)
        _logger.info('Checked status of %d members (%d up) in %3gs' %
                     (len(members), len(ups), time.time() - mark))
        return [ups, downs]

    def disable_leader(self, members):
        ups, _ = self.ping_ensemble(members)
        for member in ups:
            if member.is_leader():
                self.disable(member)
                break

    def disable_follower(self, members):
        ups, _ = self.ping_ensemble(members)
        if len(ups) > self.quorum_size:
            candidates = sorted([m for m in ups if m.is_follower()],
                                key=EnsembleMember.last_disabled_sort_key)
            # Pick one of the candidates which haven't been disabled
            # for the longest time.
            pick = random.choice([
                m for m in candidates
                if m.last_disabled == candidates[0].last_disabled
            ])
            self.disable(pick)
        else:
            _logger.info(
                'Not disabling any follower for quorum size %d, ' +
                'alive members: %s', self.quorum_size, ups)

    def enable_all(self, members):
        _, downs = self.ping_ensemble(members)
        for member in downs:
            self.enable(member)


class RandomDispatcher(AbstractDispatcher):
    def __init__(self, **kwargs):
        super(RandomDispatcher, self).__init__(**kwargs)

    def _decide(self, members, ups, downs, *, quorum_size):
        n_up = len(ups)

        if n_up <= quorum_size:
            action = ACTION_ENABLE
        elif n_up >= len(members):
            action = ACTION_DISABLE
        else:
            action = random.choice([ACTION_ENABLE, ACTION_DISABLE])

        if action == ACTION_ENABLE:
            member = random.choice(downs)
        else:
            member = random.choice(ups)

        return [action, member]

    def run(self, hosts_and_ports, quorum_size):
        members = [EnsembleMember(hp) for hp in hosts_and_ports]
        action = None
        while True:
            self.sleep_after(action)
            ups, downs = self.ping_ensemble(members)
            action, member = self._decide(
                members, ups, downs, quorum_size=quorum_size)
            _logger.debug(
                "Dispatcher has decided to %s member %s" % (action, member))
            if action == ACTION_DISABLE:
                self.disable(member=member)
            elif action == ACTION_ENABLE:
                self.enable(member=member)


class ProgrammedDispatcher(AbstractDispatcher):
    def __init__(self, *, program, **kwargs):
        super(ProgrammedDispatcher, self).__init__(**kwargs)

        if isinstance(program, str):
            self.program = self._parse(program)
        else:
            self.program = program

        self.pc = 0

    def run(self, hosts_and_ports, quorum_size):
        self.members = [EnsembleMember(hp) for hp in hosts_and_ports]
        self.quorum_size = quorum_size
        while True:
            instr = self.program[self.pc]
            _logger.debug("Executing instruction[%d]: %s" % (self.pc, instr))
            f = getattr(self, '_op_' + instr[0])
            f(*instr[1:])
            self.pc += 1
            if self.pc >= len(self.program):
                self.pc = 0

    def _op_poll_initial_hatch_complete(self, sleep_ms):
        self.wait_initial_hatch_complete(int(sleep_ms))

    def _op_sleep(self, sleep_ms):
        self.sleep_ms(int(sleep_ms))

    def _op_disable(self, member_at):
        self.disable(self.members[int(member_at)])

    def _op_enable(self, member_at):
        self.enable(self.members[int(member_at)])

    def _op_poll_up(self, member_at, sleep_ms):
        member = self.members[int(member_at)]
        while not member.is_up():
            self.sleep_ms(int(sleep_ms))
            member.ping()

    def _op_disable_leader(self):
        self.disable_leader(self.members)

    def _op_disable_follower(self):
        self.disable_follower(self.members)

    def _op_enable_all(self):
        self.enable_all(self.members)

    def _parse(self, program_text):
        program = []
        for line in re.split(r'\s*\n\s*', program_text):
            if line == '' or line.startswith('#'):
                continue
            program.append(re.split(r'\s+', line))
        _logger.debug("Parsed program %s" % program)
        return program


class FunctionDispatcher(AbstractDispatcher):
    def __init__(self, *, fn, **kwargs):
        super(FunctionDispatcher, self).__init__(**kwargs)

        self.fn = fn

    def run(self, hosts_and_ports, quorum_size):
        members = [EnsembleMember(hp) for hp in hosts_and_ports]
        self.fn(
            controller=self,
            hosts_and_ports=hosts_and_ports,
            quorum_size=quorum_size,
            members=members)


def run_dispatcher_in_master(dispatcher, fn):
    while not locust.runners.locust_runner:
        gevent.sleep(0.1)
    if isinstance(locust.runners.locust_runner,
                  locust.runners.SlaveLocustRunner):
        return

    if not dispatcher:
        if fn:
            dispatcher = FunctionDispatcher(fn=fn)
        elif _config_program:
            dispatcher = ProgrammedDispatcher(program=_config_program)
        else:
            dispatcher = RandomDispatcher()

    hosts_and_ports = split_zk_hosts()
    quorum_size = (len(hosts_and_ports) // 2) + 1
    _logger.info("Running dispatcher %s with hosts %s and quorum size %d" %
                 (dispatcher, hosts_and_ports, quorum_size))

    dispatcher.run(hosts_and_ports, quorum_size)


def register_dispatcher(*, dispatcher=None, fn=None):
    gevent.spawn(run_dispatcher_in_master, dispatcher, fn)
