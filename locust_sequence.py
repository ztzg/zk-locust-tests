from datetime import timedelta

import gevent
from gevent import GreenletExit

import locust.runners

from zk_locust import ZKLocust, ZKLocustTaskSequence
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import duration
from zk_locust.task_sets import ZKConnectTaskSet, ZKSetTaskSet, ZKGetTaskSet, ZKSetAndGetTaskSet, ZKCreateAndDeleteTaskSet, ZKWatchTaskSet, ZKExistsTaskSet, ZKExistsManyTaskSet, ZKGetChildrenTaskSet, ZKGetChildren2TaskSet

register_extra_stats()
register_zk_metrics()


def startup(*args, **kwargs):
    gevent.sleep(10)


def shutdown(*args, **kwargs):
    if False:
        locust_runner = locust.runners.locust_runner
        locust_runner.quit()
    else:
        raise GreenletExit()


class Sequence(ZKLocust):
    class task_set(ZKLocustTaskSequence):
        def __init__(self, parent):
            super(Sequence.task_set, self).__init__(parent)

            s = timedelta(seconds=5)

            def run_connect(task_set):
                ZKConnectTaskSet(task_set, maybe_interrupt=duration(s)).run()

            def run_set(task_set):
                ZKSetTaskSet(task_set, maybe_interrupt=duration(s)).run()

            def run_get(task_set):
                ZKGetTaskSet(task_set, maybe_interrupt=duration(s)).run()

            def run_set_and_get(task_set):
                ZKSetAndGetTaskSet(task_set, maybe_interrupt=duration(s)).run()

            def run_create_and_delete(task_set):
                ZKCreateAndDeleteTaskSet(
                    task_set, maybe_interrupt=duration(s)).run()

            def run_watch(task_set):
                ZKWatchTaskSet(task_set, maybe_interrupt=duration(s)).run()

            def run_exists(task_set):
                ZKExistsTaskSet(task_set, maybe_interrupt=duration(s)).run()

            def run_exists_many(task_set):
                ZKExistsManyTaskSet(
                    task_set, maybe_interrupt=duration(s)).run()

            def run_get_children(task_set):
                ZKGetChildrenTaskSet(
                    task_set, maybe_interrupt=duration(s)).run()

            def run_get_children2(task_set):
                ZKGetChildren2TaskSet(
                    task_set, maybe_interrupt=duration(s)).run()

            self.tasks = [
                startup, run_set, run_get, run_set_and_get,
                run_create_and_delete, run_watch, run_exists, run_exists_many,
                run_get_children, run_get_children2, shutdown
            ]
