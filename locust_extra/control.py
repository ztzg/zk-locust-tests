import json

import gevent
import gevent.event

import locust.runners
from locust import events

controller_available = events.EventHook()

_user_count = None
_hatch_complete = gevent.event.Event()

_prefix = "locust-extra://"

_STATE_STOPPING = locust.runners.STATE_STOPPING
_STATE_STOPPED = locust.runners.STATE_STOPPED


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
    runners = locust.runners
    while not runners.locust_runner:
        gevent.sleep(0.01)
    if isinstance(runners.locust_runner, runners.LocalLocustRunner):
        _startup(runners.locust_runner, fn)
    elif isinstance(runners.locust_runner, runners.MasterLocustRunner):
        _startup(runners.locust_runner, fn)
    # else: abandon greenlet.


def register_controller(fn=None):
    gevent.spawn(_controller_poll_runner, fn)


class Controllee(object):
    def __init__(self, parameters):
        self.parameters = parameters

    def wait_local_hatch_complete(self):
        _hatch_complete.wait()


def ensure_controllee(locust):
    # See KLUDGE note above.
    encoded = locust.host
    if not encoded or not encoded.startswith(_prefix):
        raise gevent.GreenletExit
    parameters = json.loads(encoded[len(_prefix):])
    return Controllee(parameters)
