from collections import deque

from zk_locust import ZKLocustTaskSet
from zk_locust.ops import ZKSetOp, ZKIncrementingSetOp, ZKGetOp, ZKConnectOp, ZKCreateEphemeralOp, ZKDeleteFromQueueOp, ZKCountChildrenOp, ZKExistsOp, ZKExistsWithWatchOp, ZKExistsWithManyWatchesOp, ZKWatchOp, ZKGetChildrenOp, ZKGetChildren2Op


class ZKConnectTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='connect', **kwargs):
        super(ZKConnectTaskSet, self).__init__(parent, **kwargs)

        connect_op = ZKConnectOp(self.client, task_set_name=name)

        self.tasks = [connect_op.task]


class ZKSetTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='set', val_size=None, **kwargs):
        super(ZKSetTaskSet, self).__init__(parent, **kwargs)

        op = ZKSetOp(self.client, task_set_name=name, val_size=val_size)

        self.tasks = [op.task]


class ZKGetTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='get', val_size=None, **kwargs):
        super(ZKGetTaskSet, self).__init__(parent, **kwargs)

        op = ZKGetOp(self.client, task_set_name=name, val_size=val_size)

        self.tasks = [op.task]


class ZKSetAndGetTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='set_and_get', val_size=None, **kwargs):
        super(ZKSetAndGetTaskSet, self).__init__(parent, **kwargs)

        set_op = ZKIncrementingSetOp(
            self.client, task_set_name=name, val_size=val_size)
        get_op = ZKGetOp(self.client, task_set_name=name, val_size=val_size)

        # KLUDGE: Locust's dictionary approach does not work with
        # constructors.
        self.tasks = [set_op.task] + [get_op.task for i in range(10)]


class ZKCreateAndDeleteTaskSet(ZKLocustTaskSet):
    def __init__(self,
                 parent,
                 *,
                 name='create_and_delete',
                 val_size=None,
                 **kwargs):
        super(ZKCreateAndDeleteTaskSet, self).__init__(parent, **kwargs)

        do_delete = deque()

        create_op = ZKCreateEphemeralOp(
            self.client,
            push=do_delete.append,
            task_set_name=name,
            val_size=val_size)
        delete_op = ZKDeleteFromQueueOp(
            self.client, pop=do_delete.popleft, task_set_name=name)
        count_op = ZKCountChildrenOp(self.client, task_set_name=name)

        # KLUDGE: Locust's dictionary approach does not work with
        # constructors.
        create_tasks = [create_op.task for i in range(75)]
        delete_tasks = [delete_op.task for i in range(125)]

        self.tasks = create_tasks + delete_tasks + [count_op.task]


class ZKWatchTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='watch', **kwargs):
        super(ZKWatchTaskSet, self).__init__(parent, **kwargs)

        path = self.client.create_default_node()

        op = ZKWatchOp(self.client, path, task_set_name=name)

        self.tasks = [op.task]


class ZKExistsTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='exists', **kwargs):
        super(ZKExistsTaskSet, self).__init__(parent, **kwargs)

        good_path = self.client.create_default_node()
        bad_path = self.client.join_path('/doesnotexist')

        pos_op = ZKExistsOp(
            self.client,
            good_path,
            request_type='exists_positive',
            task_set_name=name)
        neg_op = ZKExistsOp(
            self.client,
            bad_path,
            request_type='exists_negative',
            task_set_name=name)

        posw_op = ZKExistsWithWatchOp(
            self.client,
            good_path,
            request_type='exists_positive_watch',
            task_set_name=name)
        negw_op = ZKExistsWithWatchOp(
            self.client,
            bad_path,
            request_type='exists_negative_watch',
            task_set_name=name)

        self.tasks = [pos_op.task, neg_op.task, posw_op.task, negw_op.task]


class ZKExistsManyTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='exists_many', **kwargs):
        super(ZKExistsManyTaskSet, self).__init__(parent, **kwargs)

        op = ZKExistsWithManyWatchesOp(self.client, task_set_name=name)

        self.tasks = [op.task]


class ZKGetChildrenTaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='get_children', **kwargs):
        super(ZKGetChildrenTaskSet, self).__init__(parent, **kwargs)

        path = self.client.join_path('/')
        self.client.create_default_node()

        op = ZKGetChildrenOp(self.client, path, task_set_name=name)

        self.tasks = [op.task]


class ZKGetChildren2TaskSet(ZKLocustTaskSet):
    def __init__(self, parent, *, name='get_children2', **kwargs):
        super(ZKGetChildren2TaskSet, self).__init__(parent, **kwargs)

        path = self.client.join_path('/')
        self.client.create_default_node()

        op = ZKGetChildren2Op(self.client, path, task_set_name=name)

        self.tasks = [op.task]
