import gevent

import locust.runners
from locust import events

controller_available = events.EventHook()

RUNNER_LOCAL, RUNNER_MASTER, RUNNER_SLAVE = ["local", "master", "slave"]

_initial_hatch_complete = False


def on_hatch_complete(user_count):
    global _initial_hatch_complete
    _initial_hatch_complete = True


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
    def __init__(self, runner):
        self.runner = runner
        self.client_count = None

    def wait_initial_hatch_complete(self):
        while not _initial_hatch_complete:
            gevent.sleep(0.25)

    def get_num_clients(self):
        return self.runner.num_clients

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
