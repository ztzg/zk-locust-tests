import json

import gevent
import gevent.event

import locust.runners
from locust import events

controller_available = events.EventHook()

RUNNER_LOCAL, RUNNER_MASTER, RUNNER_SLAVE = ["local", "master", "slave"]

_user_count = None
_hatch_complete = gevent.event.Event()

_prefix = "locust-extra://"

_STATE_STOPPING = locust.runners.STATE_STOPPING
_STATE_STOPPED = locust.runners.STATE_STOPPED


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


def on_start_hatching():
    _hatch_complete.clear()


events.master_start_hatching += on_start_hatching
events.locust_start_hatching += on_start_hatching


def on_hatch_complete(user_count):
    global _user_count
    _user_count = user_count
    _hatch_complete.set()


events.hatch_complete += on_hatch_complete


class Controller(object):
    def __init__(self, runner):
        self.runner = runner
        self.client_count = None

    def wait_hatch_complete(self):
        _hatch_complete.wait()
        if self.client_count is None:
            self.client_count = _user_count

    def wait_initial_hatch_complete(self):
        if self.client_count is None:
            self.wait_hatch_complete()
        return self.client_count

    def stop_runner(self):
        _hatch_complete.clear()
        runner = self.runner
        runner.stop()
        while runner.state != _STATE_STOPPED:
            assert runner.state == _STATE_STOPPING
            gevent.sleep(0.01)

    def start_runner(self, parameters, *, wait=False):
        runner = self.runner
        encoded = json.dumps(parameters)
        # KLUDGE: The only "client-controllable" information which
        # flows from master to workers is the "host."  We don't use
        # that parameter in the intended way, but rather stuff the
        # data we want to carry over into it, encoded in JSON!
        runner.host = _prefix + encoded
        runner.start_hatching(runner.num_clients, runner.hatch_rate)
        if wait:
            self.wait_hatch_complete()

    def restart_with_parameters(self, parameters, *, wait=False):
        self.stop_runner()
        self.start_runner(parameters, wait=wait)


def _startup(runner, fn):
    controller = Controller(runner)
    controller_available.fire(controller)
    if fn:
        fn(controller)


def _controller_poll_runner(fn):
    kind = _wait_runner_kind()
    if kind in [RUNNER_LOCAL, RUNNER_MASTER]:
        _startup(locust.runners.locust_runner, fn)
    # else: abandon greenlet.


def register_controller(fn=None):
    gevent.spawn(_controller_poll_runner, fn)


class Controllee(object):
    def __init__(self, parameters):
        self.parameters = parameters

    def wait_local_hatch_complete(self):
        _hatch_complete.wait()


def _wait_or_shutdown():
    if _wait_runner_kind() is RUNNER_SLAVE:
        raise gevent.GreenletExit
    else:
        # BUG: LocalLocustRunner instances don't like it when we stop,
        # and just exit as soon as the greenlets have shut down.  (The
        # Web UI case is okay, because the HTTP server greenlet
        # continues to serve.)  This hack doesn't seem to fix it.
        # TODO(ddiederen): Figure out a workaround.
        while True:
            gevent.sleep(0.3)


def ensure_controllee(locust):
    # See KLUDGE note above.
    encoded = locust.host
    if not encoded or not encoded.startswith(_prefix):
        _wait_or_shutdown()
    parameters = json.loads(encoded[len(_prefix):])
    return Controllee(parameters)
