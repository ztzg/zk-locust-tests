import sys
import os

from datetime import timedelta

sys.path.append(os.getcwd())  # See "Common libraries" in Locust docs.
from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import duration, ZKSimpleGet

register_extra_stats()
register_zk_metrics()


class Get(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(Get.task_set, self).__init__(parent)

            maybe_interrupt = None
            if False:
                maybe_interrupt = duration(timedelta(seconds=15))

            self._op = ZKSimpleGet(
                self.client, maybe_interrupt=maybe_interrupt)
            self.tasks = [self._op.task]
