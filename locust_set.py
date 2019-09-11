from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKSetOp

register_extra_stats()
register_zk_metrics()


class Set(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(Set.task_set, self).__init__(parent)

            op = ZKSetOp(self.client)

            self.tasks = [op.task]
