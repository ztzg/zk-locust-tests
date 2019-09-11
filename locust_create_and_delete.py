from collections import deque

from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKCreateEphemeralOp, ZKDeleteFromQueueOp, \
    ZKCountChildrenOp

register_extra_stats()
register_zk_metrics()


class CreateAndDelete(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(CreateAndDelete.task_set, self).__init__(parent)

            do_delete = deque()

            create_op = ZKCreateEphemeralOp(self.client, push=do_delete.append)
            delete_op = ZKDeleteFromQueueOp(self.client, pop=do_delete.popleft)
            count_op = ZKCountChildrenOp(self.client)

            # KLUDGE: Locust's dictionary approach does not work with
            # constructors.
            create_tasks = [create_op.task for i in range(75)]
            delete_tasks = [delete_op.task for i in range(125)]

            self.tasks = create_tasks + delete_tasks + [count_op.task]
