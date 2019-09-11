from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKSetAndGetTaskSet

register_extra_stats()
register_zk_metrics()


class SetAndGet(ZKLocust):
    task_set = ZKSetAndGetTaskSet
