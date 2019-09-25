import logging

from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKGetTaskSet
from zk_dispatch import register_dispatcher

logging.basicConfig()
logging.getLogger('zk_dispatch').setLevel(logging.DEBUG)
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

register_extra_stats()
register_zk_metrics()
register_dispatcher()


class Get(ZKLocust):
    task_set = ZKGetTaskSet
