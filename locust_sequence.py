import os

from datetime import timedelta

import gevent
from gevent import GreenletExit

from kazoo.recipe.barrier import DoubleBarrier

import locust.runners
from locust import events

from zk_locust import ZKLocust, ZKLocustTaskSequence
from zk_locust.backend_kazoo import KazooLocustClient
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import duration
from zk_locust.task_sets import ZKConnectTaskSet, ZKSetTaskSet, ZKGetTaskSet, ZKSetAndGetTaskSet, ZKCreateAndDeleteTaskSet, ZKWatchTaskSet, ZKExistsTaskSet, ZKExistsManyTaskSet, ZKGetChildrenTaskSet, ZKGetChildren2TaskSet

register_extra_stats()
register_zk_metrics()


def get_num_clients():
    # Note: we cannot determine the number of clients automatically in
    # the distributed case (the 'hatch_complete' event is per-slave!).
    # See -c/--clients parameter of the wrapper script, or set
    # ZK_LOCUST_NUM_CLIENTS to the correct value.
    global _num_clients
    v = os.getenv('ZK_LOCUST_NUM_CLIENTS')
    if not v:
        return None
    return int(v)


_num_clients = get_num_clients()

_bench_repetitions = int(os.getenv('ZK_LOCUST_BENCH_REPETITIONS') or '1')
_bench_step_duration = float(
    os.getenv('ZK_LOCUST_BENCH_STEP_DURATION') or '10')
_bench_barrier_hosts = os.getenv('ZK_LOCUST_BENCH_BARRIER_HOSTS')
_bench_barrier_path = os.getenv('ZK_LOCUST_BENCH_BARRIER_PATH', '/kl_barrier')

# _bench_val_sizes = [None]
_bench_val_sizes = [8, 512, 8 * 1024]


def startup(*args, **kwargs):
    pass


def shutdown(*args, **kwargs):
    locust_runner = locust.runners.locust_runner
    if isinstance(locust_runner, locust.runners.SlaveLocustRunner):
        raise GreenletExit()
    else:
        locust_runner.quit()


def barrier_wrap(zk_client, task):
    if _num_clients is None or _num_clients <= 0:
        raise ValueError('Invalid client count: %s' % str(_num_clients))

    def fn(task_set):
        barrier = DoubleBarrier(zk_client, _bench_barrier_path, _num_clients)
        barrier.enter()
        try:
            gevent.sleep(0.5)
            task(task_set)
            gevent.sleep(0.5)
        finally:
            barrier.leave()

    return fn


def gen_val_size_tasks(s, val_size):
    suffix = None

    if val_size:
        if val_size >= 1024:
            suffix = '%s_KiB' % round(val_size / 1024, 3)
        else:
            suffix = '%s_B' % val_size

    def run_set(task_set):
        ZKSetTaskSet(
            task_set,
            suffix=suffix,
            val_size=val_size,
            maybe_interrupt=duration(s)).run()

    def run_get(task_set):
        ZKGetTaskSet(
            task_set,
            suffix=suffix,
            val_size=val_size,
            maybe_interrupt=duration(s)).run()

    def run_set_and_get(task_set):
        ZKSetAndGetTaskSet(
            task_set,
            suffix=suffix,
            val_size=val_size,
            maybe_interrupt=duration(s)).run()

    def run_create_and_delete(task_set):
        ZKCreateAndDeleteTaskSet(
            task_set,
            suffix=suffix,
            val_size=val_size,
            maybe_interrupt=duration(s)).run()

    return [run_set, run_get, run_set_and_get, run_create_and_delete]


class Sequence(ZKLocust):
    class task_set(ZKLocustTaskSequence):
        def __init__(self, parent):
            super(Sequence.task_set, self).__init__(parent)

            s = timedelta(seconds=_bench_step_duration)
            tasks = []

            def run_connect(task_set):
                ZKConnectTaskSet(task_set, maybe_interrupt=duration(s)).run()

            for val_size in _bench_val_sizes:
                tasks += gen_val_size_tasks(s, val_size)

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

            tasks += [
                run_watch, run_exists, run_exists_many, run_get_children,
                run_get_children2
            ]

            if _bench_barrier_path:
                if _bench_barrier_hosts:
                    client = KazooLocustClient(
                        hosts=_bench_barrier_hosts,
                        pseudo_root=None,
                        autostart=True)
                else:
                    client = self.client
                zk_client = client.get_zk_client()

                tasks = [barrier_wrap(zk_client, task) for task in tasks]

            tasks *= _bench_repetitions

            self.tasks = [startup] + tasks + [shutdown]
