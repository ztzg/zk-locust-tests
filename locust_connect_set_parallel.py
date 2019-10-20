# A "locustfile" which tries to determine the impact of session
# creations (caused by Connect) on the rate of "normal" operations
# (from Set, in this case).  The 'weight' attribute is used to
# configure a ~1:10 ratio.

import logging

from zk_locust import ZKLocust
from zk_locust.task_sets import ZKConnectTaskSet, ZKSetTaskSet
from locust_extra.stats import register_extra_stats

from zk_metrics import register_zk_metrics

logging.basicConfig()
logging.getLogger('locust_extra.control').setLevel(logging.DEBUG)
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

register_zk_metrics()
register_extra_stats()


class Connect(ZKLocust):
    weight = 1
    task_set = ZKConnectTaskSet

    def __init__(self):
        # Unlike other locust instances, this one must not "autostart"
        # the ZK client.
        super(Connect, self).__init__(pseudo_root=None, autostart=False)


class Set(ZKLocust):
    weight = 10
    task_set = ZKSetTaskSet
