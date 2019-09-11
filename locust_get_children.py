from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKGetChildrenOp

register_extra_stats()
register_zk_metrics()


class GetChildren(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(GetChildren.task_set, self).__init__(parent)

            path = self.client.join_path('/')
            self.client.create_default_node()

            op = ZKGetChildrenOp(self.client, path)

            self.tasks = [op.task]
