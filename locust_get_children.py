from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKGetChildrenTaskSet

register_extra_stats()
register_zk_metrics()


class GetChildren(ZKLocust):
    task_set = ZKGetChildrenTaskSet
