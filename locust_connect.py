from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKIncrementingSetOp, ZKConnectOp

register_extra_stats()
register_zk_metrics()


class Connect(ZKLocust):
    def __init__(self):
        super(Connect, self).__init__(pseudo_root=None, autostart=False)

    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(Connect.task_set, self).__init__(parent)

            op = ZKConnectOp(self.client)

            self.tasks = [op.task]
