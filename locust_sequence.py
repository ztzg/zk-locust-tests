import os

import gevent

from zk_locust import ZKLocust, ZKLocustTaskSequence
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKConnectTaskSet, ZKSetTaskSet, ZKGetTaskSet, ZKSetAndGetTaskSet, ZKCreateAndDeleteTaskSet, ZKWatchTaskSet, ZKExistsTaskSet, ZKExistsManyTaskSet, ZKGetChildrenTaskSet, ZKGetChildren2TaskSet

from locust_extra.control import register_controller, ensure_controllee

register_extra_stats()
register_zk_metrics()

_bench_repetitions = int(os.getenv('ZK_LOCUST_BENCH_REPETITIONS') or '1')
_bench_step_duration = float(
    os.getenv('ZK_LOCUST_BENCH_STEP_DURATION') or '10')

# _bench_val_sizes = [None]
_bench_val_sizes = [8, 512, 8 * 1024]

_bench_include = None

# _bench_include = ['run_set', 'run_get']


def _gen_val_size_tasks(val_size):
    suffix = None

    if val_size:
        if val_size >= 1024:
            suffix = '%s_KiB' % round(val_size / 1024, 3)
        else:
            suffix = '%s_B' % val_size

    def run_set(task_set):
        ZKSetTaskSet(task_set, suffix=suffix, val_size=val_size).run()

    def run_get(task_set):
        ZKGetTaskSet(task_set, suffix=suffix, val_size=val_size).run()

    def run_set_and_get(task_set):
        ZKSetAndGetTaskSet(task_set, suffix=suffix, val_size=val_size).run()

    def run_create_and_delete(task_set):
        ZKCreateAndDeleteTaskSet(
            task_set, suffix=suffix, val_size=val_size).run()

    return [run_set, run_get, run_set_and_get, run_create_and_delete]


def _gen_tasks():
    tasks = []

    def run_connect(task_set):
        ZKConnectTaskSet(task_set).run()

    for val_size in _bench_val_sizes or [None]:
        tasks += _gen_val_size_tasks(val_size)

    def run_watch(task_set):
        ZKWatchTaskSet(task_set).run()

    def run_exists(task_set):
        ZKExistsTaskSet(task_set).run()

    def run_exists_many(task_set):
        ZKExistsManyTaskSet(task_set).run()

    def run_get_children(task_set):
        ZKGetChildrenTaskSet(task_set).run()

    def run_get_children2(task_set):
        ZKGetChildren2TaskSet(task_set).run()

    tasks += [
        run_watch, run_exists, run_exists_many, run_get_children,
        run_get_children2
    ]

    if _bench_include:
        tasks = [task for task in tasks if task.__name__ in _bench_include]

    tasks *= _bench_repetitions

    return tasks


_all_tasks = _gen_tasks()


def _controller_available(controller):
    controller.wait_initial_hatch_complete()
    for i in range(len(_all_tasks)):
        controller.restart_with_parameters({'task_indices': [i]}, wait=True)
        gevent.sleep(_bench_step_duration)
    controller.runner.quit()


register_controller(_controller_available)


class Sequence(ZKLocust):
    class task_set(ZKLocustTaskSequence):
        def __init__(self, parent):
            super(Sequence.task_set, self).__init__(parent)

            controllee = ensure_controllee(self.locust)
            task_indices = controllee.parameters['task_indices']

            self.tasks = [_all_tasks[i] for i in task_indices]

            controllee.wait_local_hatch_complete()
