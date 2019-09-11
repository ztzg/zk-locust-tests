from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKExistsManyTaskSet

register_extra_stats()
register_zk_metrics()


class ExistsMany(ZKLocust):
    task_set = ZKExistsManyTaskSet
