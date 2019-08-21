from collections import deque

from locust import TaskSet, task

import kazoo

from common import KazooLocust, LocustTimer


class CreateAndDelete(KazooLocust):
    min_wait = 0
    max_wait = 0

    class task_set(TaskSet):
        _to_delete = deque()

        def __init__(self, parent):
            super(CreateAndDelete.task_set, self).__init__(parent)

            self._k = self.client.get_kazoo_client()

            try:
                self._k.create('/kx')
            except kazoo.exceptions.NodeExistsError:
                pass

        @task(100)
        def zk_create(self):
            with LocustTimer('create') as ctx:
                k = self._k.create('/kx/c-', ephemeral=True, sequence=True)
                ctx.success()
            self._to_delete.append(k)

        @task(100)
        def zk_delete(self):
            k = None
            try:
                k = self._to_delete.popleft()
            except IndexError:
                pass

            if k:
                with LocustTimer('delete') as ctx:
                    self._k.delete(k)
                    ctx.success()

        @task(1)
        def zk_children_count(self):
            with LocustTimer('children_count') as ctx:
                s = self._k.exists('/kx')
                ctx.success(response_length=s.children_count)
