from zk_locust import ZKLocust, ZKLocustTaskSet
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.ops import ZKExistsOp, ZKExistsWithWatchOp

register_extra_stats()
register_zk_metrics()


class Exists(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(Exists.task_set, self).__init__(parent)

            good_path = self.client.create_default_node()
            bad_path = self.client.join_path('/doesnotexist')

            pos_op = ZKExistsOp(self.client, good_path, 'exists_positive')
            neg_op = ZKExistsOp(self.client, bad_path, 'exists_negative')

            posw_op = ZKExistsWithWatchOp(self.client, good_path,
                                          'exists_positive_watch')
            negw_op = ZKExistsWithWatchOp(self.client, bad_path,
                                          'exists_negative_watch')

            self.tasks = [pos_op.task, neg_op.task, posw_op.task, negw_op.task]
