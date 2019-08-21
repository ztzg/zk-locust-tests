from locust import TaskSet, task


from common import KazooLocust, LocustTimer


class GetChildren(KazooLocust):
    min_wait = 0
    max_wait = 0

    class task_set(TaskSet):
        def __init__(self, parent):
            super(GetChildren.task_set, self).__init__(parent)

            self._k = self.client.get_kazoo_client()
            self._n = self.client.create_default_node()

        @task
        def zk_get_children(self):
            with LocustTimer('get_children') as ctx:
                c = self._k.get_children('/kl')
                ctx.success(len(c))
