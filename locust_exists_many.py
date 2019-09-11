from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKExistsOp, ZKExistsWithManyWatchesOp

register_extra_stats()
register_zk_metrics()


class ExistsMany(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(ExistsMany.task_set, self).__init__(parent)

            op = ZKExistsWithManyWatchesOp(self.client)

            self.tasks = [op.task]
