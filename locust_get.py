from datetime import timedelta

from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import duration, ZKGetOp

register_extra_stats()
register_zk_metrics()


class Get(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(Get.task_set, self).__init__(parent)

            maybe_interrupt = None
            if False:
                maybe_interrupt = duration(timedelta(seconds=15))

            op = ZKGetOp(self.client, maybe_interrupt=maybe_interrupt)

            self.tasks = [op.task]
