import os
import logging
import time

import gevent.queue

from kazoo.protocol.states import KazooState
from kazoo.exceptions import ConnectionLoss, SessionExpiredError

from locust import events, task
from locust.exception import StopLocust

from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_dispatch import register_dispatcher

_disable_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_DISABLE_MS', '15000'))
_conn_tm_ms = int(os.getenv('ZK_LOCUST_BENCH_CONNECT_TIMEOUT_MS', '60000'))

logging.basicConfig()
logging.getLogger('zk_dispatch').setLevel(logging.DEBUG)
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

# Ignore connection issues when trying to gather metrics
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

register_extra_stats()
register_zk_metrics()


def _ensemble_handler(controller, members, **kwargs):
    controller.wait_initial_hatch_complete()
    controller.sleep_ms(_disable_ms)
    controller.disable_leader(members)


register_dispatcher(fn=_ensemble_handler)


class InitialConnectTimeout(Exception):
    pass


class UnexpectedConnect(Exception):
    pass


class UnexpectedState(Exception):
    pass


class StandbyTaskSet(ZKLocustTaskSet):
    def __init__(self, parent):
        super(StandbyTaskSet, self).__init__(parent)

        self.request_type = 'standby'

        self.noted_suspends = 0

        self.queue = gevent.queue.Queue()

        self.kazoo = self.client.get_zk_client(stopped_ok=True)

        # A listener which simply logs transitions
        def state_listener(state):
            now = time.time()
            self.queue.put((self.prev_time, now, self.prev_state, state))
            self.prev_state = state
            self.prev_time = now

        self.kazoo.add_listener(state_listener)

        # Note time and start connecting
        self.prev_state = self.kazoo.state
        self.prev_time = time.time()
        result = self.kazoo.start_async()
        result.wait(_conn_tm_ms / 1000)

        if not self.kazoo.connected:
            # Abandon
            self.kazoo.stop()
            self.kazoo.close()
            raise InitialConnectTimeout()

    @task
    def task(self):
        # This "task" simply pops state transitions from the queue
        prev_time, state_time, prev_state, state = self.queue.get()
        exc = None

        if state == KazooState.CONNECTED:
            if self.noted_suspends <= 1:
                name = "reconnect" if self.noted_suspends else "connect"

                events.request_success.fire(
                    request_type=self.request_type,
                    name=name,
                    response_time=int((state_time - prev_time) * 1000),
                    response_length=1)
                return

            exc = UnexpectedConnect()
        elif state == KazooState.SUSPENDED:
            self.noted_suspends += 1

            if self.noted_suspends == 1:
                # Suspend of interest
                return

            exc = ConnectionLoss()
        elif state == KazooState.LOST:
            exc = SessionExpiredError()
        else:
            exc = UnexpectedState()

        events.request_failure.fire(request_type=self.request_type,
                                    name="failure",
                                    response_time=1,
                                    exception=exc)

        raise StopLocust()


class Standby(ZKLocust):
    min_wait = 1
    max_wait = 1
    task_set = StandbyTaskSet

    def __init__(self):
        # Let's not "autostart" the ZK client before a listener is
        # attached.
        super(Standby, self).__init__(pseudo_root=None, autostart=False)
