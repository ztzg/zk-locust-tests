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

        self.http_session.get_adapter(self.ping_url).max_retries = 1

    def __str__(self):
        s = self.host_and_port + ' (state ' + self.state
        if self.last_ping:
            s += ' %3gs ago' % (time.time() - self.last_ping)
        s += ')'
        return s

    def is_up(self):
        return self.state in _MEMBER_STATES_UP

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
        self.controller.disable(member)

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


class RandomDispatcher(AbstractDispatcher):
    def __init__(self, **kwargs):
        super(RandomDispatcher, self).__init__(**kwargs)

    def _ping_ensemble(self, members):
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
            ups, downs = self._ping_ensemble(members)
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
        while True:
            instr = self.program[self.pc]
            _logger.debug("Executing instruction[%d]: %s" % (self.pc, instr))
            f = getattr(self, '_op_' + instr[0])
            f(*instr[1:])
            self.pc += 1
            if self.pc >= len(self.program):
                self.pc = 0

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

    def _parse(self, program_text):
        program = []
        for line in re.split(r'\s*\n\s*', program_text):
            if line == '' or line.startswith('#'):
                continue
            program.append(re.split(r'\s+', line))
        _logger.debug("Parsed program %s" % program)
        return program


def run_dispatcher_in_master(dispatcher):
    while not locust.runners.locust_runner:
        gevent.sleep(0.1)
    if isinstance(locust.runners.locust_runner,
                  locust.runners.SlaveLocustRunner):
        return

    if not dispatcher:
        if _config_program:
            dispatcher = ProgrammedDispatcher(program=_config_program)
        else:
            dispatcher = RandomDispatcher()

    hosts_and_ports = split_zk_hosts()
    quorum_size = (len(hosts_and_ports) // 2) + 1
    _logger.info("Running dispatcher %s with hosts %s and quorum size %d" %
                 (dispatcher, hosts_and_ports, quorum_size))

    dispatcher.run(hosts_and_ports, quorum_size)


def register_dispatcher(*, dispatcher=None):
    gevent.spawn(run_dispatcher_in_master, dispatcher)
