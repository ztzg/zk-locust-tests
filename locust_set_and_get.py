from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKIncrementingSetOp, ZKGetOp

register_extra_stats()
register_zk_metrics()


class SetAndGet(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(SetAndGet.task_set, self).__init__(parent)

            set_op = ZKIncrementingSetOp(self.client)
            get_op = ZKGetOp(self.client)

            # KLUDGE: Locust's dictionary approach does not work with
            # constructors.
            self.tasks = [set_op.task] + [get_op.task for i in range(10)]
