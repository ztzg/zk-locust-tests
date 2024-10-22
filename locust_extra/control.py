import os
import re
import logging

import gevent

import locust.runners
from locust import events

_logger = logging.getLogger(__name__)

controller_available = events.EventHook()

RUNNER_LOCAL, RUNNER_MASTER, RUNNER_SLAVE = ["local", "master", "slave"]

_generation = 0

_config_program = os.getenv('LOCUST_EXTRA_CONTROL_PROGRAM')

_monkey_patch_hatching = int(
    os.getenv('LOCUST_EXTRA_MONKEY_PATCH_HATCHING', '1')) != 0


def _subst_hatching():
    cls = locust.runners.LocustRunner
    orig_start_hatching = cls.start_hatching

    # KLUDGE: Locust 0.11.0 looks at the wrong property (num_clients
    # instead of user_count) when computing the number of instances to
    # spawn/kill!  We try compensating for it by monkey-patching a
    # "fixed" start_hatching implementation.
    def new_start_hatching(self, *args, **kwargs):
        if (self.state != locust.runners.STATE_INIT
                and self.state != locust.runners.STATE_STOPPED):
            uc = self.user_count
            dx = self.num_clients - uc
            if dx:
                _logger.warning('Correcting for num_clients drift (%r)', dx)
                self.num_clients = uc

        return orig_start_hatching(self, *args, **kwargs)

    cls.start_hatching = new_start_hatching
    _logger.info("Monkey-patched 'start_hatching' replacement into %r", cls)


def on_hatch_complete(user_count):
    global _generation
    _generation += 1


events.hatch_complete += on_hatch_complete


def _wait_runner_kind():
    runners = locust.runners
    kind = None
    while not runners.locust_runner:
        gevent.sleep(0.01)
    if isinstance(runners.locust_runner, runners.LocalLocustRunner):
        kind = RUNNER_LOCAL
    elif isinstance(runners.locust_runner, runners.MasterLocustRunner):
        kind = RUNNER_MASTER
    elif isinstance(runners.locust_runner, runners.SlaveLocustRunner):
        kind = RUNNER_SLAVE
    assert kind
    return kind


class Controller(object):
    def __init__(self, runner, kind):
        self.runner = runner
        self._runner_kind = kind

    def wait_initial_hatch_complete(self, sleep_ms=250):
        while not _generation:
            gevent.sleep(sleep_ms / 1000)

    def sleep_ms(self, ms, cause=None):
        msg = 'Sleeping %dms' % ms
        if cause:
            msg += ' ' + cause
        _logger.debug(msg)
        gevent.sleep(ms / 1000)

    def get_num_workers(self):
        if self._runner_kind is RUNNER_MASTER:
            return self.runner.slave_count
        else:
            return 1

    def get_generation(self):
        return _generation

    def get_num_clients(self):
        return self.runner.num_clients

    def get_user_count(self):
        return self.runner.user_count

    def get_hatch_rate(self):
        return self.runner.hatch_rate

    def start_hatching(self, *, num_clients=None, hatch_rate=None):
        runner = self.runner
        runner.host = None
        if num_clients is None:
            num_clients = runner.num_clients
        if hatch_rate is None:
            hatch_rate = runner.hatch_rate

        runner.start_hatching(num_clients, hatch_rate)


class ProgrammedHandler(object):
    def __init__(self, *, program):
        if isinstance(program, str):
            self.program = self._parse(program)
        else:
            self.program = program

        self.pc = 0
        self.controller = None

    def run(self, controller):
        self.controller = controller
        self.min_num_clients = controller.get_num_clients()
        self.max_num_clients = self.min_num_clients * 4
        self.factor = None
        self.addend = None
        while True:
            instr = self.program[self.pc]
            _logger.debug("Executing instruction[%d]: %s" % (self.pc, instr))
            f = getattr(self, '_op_' + instr[0])
            f(*instr[1:])
            self.pc += 1
            if self.pc >= len(self.program):
                self.pc = 0

    def sleep_ms(self, ms, cause=None):
        self.controller.sleep_ms(ms, cause=cause)

    def change_num_clients(self, num_clients, hatch_rate=None):
        num_clients = min(self.max_num_clients,
                          max(self.min_num_clients, num_clients))
        old_num_clients = self.controller.get_num_clients()
        delta = num_clients - old_num_clients

        if not delta:
            _logger.debug('Client count unchanged: %d; constraint: [%d,%d]',
                          old_num_clients, self.min_num_clients,
                          self.max_num_clients)
            return

        hatch_rate = max(delta, 0)

        _logger.debug('Changing client count: %d -> %d (%+d; hatch_rate=%d)',
                      old_num_clients, num_clients, delta, hatch_rate)
        self.controller.start_hatching(
            num_clients=num_clients, hatch_rate=hatch_rate)

    def flip_at_bound(self):
        num_clients = self.controller.get_num_clients()

        if num_clients >= self.max_num_clients:
            return -1
        elif num_clients <= self.min_num_clients:
            return 1
        else:
            return 0

    def maybe_flip_at_bound(self, s, fallback=None):
        v = fallback
        flip = 0

        if s and s.startswith('<>'):
            flip = self.flip_at_bound()
            if flip:
                v = float(s[2:])
        elif s is not None:
            v = float(s)

        return (v, flip)

    def _op_poll_initial_hatch_complete(self, sleep_ms):
        self.wait_initial_hatch_complete(int(sleep_ms))

    def _op_sleep(self, sleep_ms):
        self.sleep_ms(int(sleep_ms))

    def _op_set_min_num_clients(self, s):
        self.min_num_clients = int(s)

    def _op_set_max_num_clients(self, s):
        self.max_num_clients = int(s)

    def _op_change_num_clients(self, num_clients_str, hatch_rate_str=None):
        hatch_rate = int(hatch_rate_str) if hatch_rate_str else None
        self.change_num_clients(int(num_clients_str), hatch_rate)

    def _op_add_num_clients(self, addend=None):
        addend, flip = self.maybe_flip_at_bound(addend, self.addend)
        if flip:
            addend = addend * flip
            self.addend = addend
        if addend is None:
            _logger.error('Cannot multiply_num_clients without a addend')
            return

        num_clients = self.controller.get_num_clients()
        new_num_clients = num_clients + addend
        self.change_num_clients(new_num_clients)

    def _op_multiply_num_clients(self, factor=None):
        factor, flip = self.maybe_flip_at_bound(factor, self.factor)
        if flip:
            factor = factor**flip
            self.factor = factor
        if factor is None:
            _logger.error('Cannot multiply_num_clients without a factor')
            return

        num_clients = self.controller.get_num_clients()
        f_num_clients = num_clients * factor
        self.change_num_clients(int(f_num_clients))

    def _parse(self, program_text):
        program = []
        for line in re.split(r'\s*\n\s*', program_text):
            if line == '' or line.startswith('#'):
                continue
            program.append(re.split(r'\s+', line))
        _logger.debug("Parsed program %s" % program)
        return program


def _startup(runner, kind, fn):
    controller = Controller(runner, kind)
    controller_available.fire(controller)
    if fn:
        fn(controller)
    elif _config_program:
        handler = ProgrammedHandler(program=_config_program)
        handler.run(controller)
    _logger.debug('Abandoning control greenlet')


def _controller_poll_runner(fn):
    kind = _wait_runner_kind()

    global _monkey_patch_hatching
    if _monkey_patch_hatching and kind in [RUNNER_LOCAL, RUNNER_SLAVE]:
        _subst_hatching()
        _monkey_patch_hatching = False

    if kind in [RUNNER_LOCAL, RUNNER_MASTER]:
        _startup(locust.runners.locust_runner, kind, fn)
    # else: Remove useless handler and abandon greenlet.
    events.hatch_complete -= on_hatch_complete


def register_controller(fn=None):
    gevent.spawn(_controller_poll_runner, fn)
