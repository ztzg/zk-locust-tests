from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKConnectTaskSet

register_extra_stats()
register_zk_metrics()


class Connect(ZKLocust):
    task_set = ZKConnectTaskSet

    def __init__(self):
        super(Connect, self).__init__(pseudo_root=None, autostart=False)
