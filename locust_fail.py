# A small "locustfile" which simulates Kazoo errors; shows the impact
# of numerous failures on reported latency.
#
# Compare by setting `--timer-exception-behavior` to `ignore` or
# `log_failure` in `parameterized-locust.sh`.

import random

import gevent

from locust import task

from zk_locust import ZKLocust, ZKLocustTaskSet, LocustTimer
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

import kazoo.exceptions

register_extra_stats()
register_zk_metrics()


class FakeKazooFail(kazoo.exceptions.KazooException):
    pass


class MostlyFailTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(MostlyFailTaskSet, self).__init__(*args, **kwargs)

        self._rg = random.Random(1571644455)

    @task
    def mostly_fail_task(self):
        p_err = 3 / 4
        with LocustTimer('op', 'mostly_fail'):
            if self._rg.random() < p_err:  # random() is uniform [0, 1).
                raise FakeKazooFail()
            else:
                gevent.sleep(0.1)


class MostlyFailLocust(ZKLocust):
    task_set = MostlyFailTaskSet
