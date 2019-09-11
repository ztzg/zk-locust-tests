from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKSetTaskSet

register_extra_stats()
register_zk_metrics()


class Set(ZKLocust):
    task_set = ZKSetTaskSet
