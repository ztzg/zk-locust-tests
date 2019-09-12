from collections import deque

from zk_locust import ZKLocustTaskSet
from zk_locust.ops import ZKSetOp, ZKIncrementingSetOp, ZKGetOp, ZKConnectOp, ZKCreateEphemeralOp, ZKDeleteFromQueueOp, ZKCountChildrenOp, ZKExistsOp, ZKExistsWithWatchOp, ZKExistsWithManyWatchesOp, ZKWatchOp, ZKGetChildrenOp, ZKGetChildren2Op


class ZKConnectTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKConnectTaskSet, self).__init__(*args, **kwargs)

        connect_op = ZKConnectOp(self.client)

        self.tasks = [connect_op.task]


class ZKSetTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKSetTaskSet, self).__init__(*args, **kwargs)

        op = ZKSetOp(self.client)

        self.tasks = [op.task]


class ZKGetTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKGetTaskSet, self).__init__(*args, **kwargs)

        op = ZKGetOp(self.client)

        self.tasks = [op.task]


class ZKSetAndGetTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKSetAndGetTaskSet, self).__init__(*args, **kwargs)

        set_op = ZKIncrementingSetOp(self.client)
        get_op = ZKGetOp(self.client, request_type='get_incr_set')

        # KLUDGE: Locust's dictionary approach does not work with
        # constructors.
        self.tasks = [set_op.task] + [get_op.task for i in range(10)]


class ZKCreateAndDeleteTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKCreateAndDeleteTaskSet, self).__init__(*args, **kwargs)

        do_delete = deque()

        create_op = ZKCreateEphemeralOp(self.client, push=do_delete.append)
        delete_op = ZKDeleteFromQueueOp(self.client, pop=do_delete.popleft)
        count_op = ZKCountChildrenOp(self.client)

        # KLUDGE: Locust's dictionary approach does not work with
        # constructors.
        create_tasks = [create_op.task for i in range(75)]
        delete_tasks = [delete_op.task for i in range(125)]

        self.tasks = create_tasks + delete_tasks + [count_op.task]


class ZKWatchTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKWatchTaskSet, self).__init__(*args, **kwargs)

        path = self.client.create_default_node()

        op = ZKWatchOp(self.client, path)

        self.tasks = [op.task]


class ZKExistsTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKExistsTaskSet, self).__init__(*args, **kwargs)

        good_path = self.client.create_default_node()
        bad_path = self.client.join_path('/doesnotexist')

        pos_op = ZKExistsOp(
            self.client, good_path, request_type='exists_positive')
        neg_op = ZKExistsOp(
            self.client, bad_path, request_type='exists_negative')

        posw_op = ZKExistsWithWatchOp(
            self.client, good_path, request_type='exists_positive_watch')
        negw_op = ZKExistsWithWatchOp(
            self.client, bad_path, request_type='exists_negative_watch')

        self.tasks = [pos_op.task, neg_op.task, posw_op.task, negw_op.task]


class ZKExistsManyTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKExistsManyTaskSet, self).__init__(*args, **kwargs)

        op = ZKExistsWithManyWatchesOp(self.client)

        self.tasks = [op.task]


class ZKGetChildrenTaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKGetChildrenTaskSet, self).__init__(*args, **kwargs)

        path = self.client.join_path('/')
        self.client.create_default_node()

        op = ZKGetChildrenOp(self.client, path)

        self.tasks = [op.task]


class ZKGetChildren2TaskSet(ZKLocustTaskSet):
    def __init__(self, *args, **kwargs):
        super(ZKGetChildren2TaskSet, self).__init__(*args, **kwargs)

        path = self.client.join_path('/')
        self.client.create_default_node()

        op = ZKGetChildren2Op(self.client, path)

        self.tasks = [op.task]
