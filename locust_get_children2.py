from locust import TaskSet, task


from common import KazooLocust, LocustTimer


class GetChildren2(KazooLocust):
    min_wait = 0
    max_wait = 0

    class task_set(TaskSet):
        def __init__(self, parent):
            super(GetChildren2.task_set, self).__init__(parent)

            self._k = self.client.get_kazoo_client()
            self._n = self.client.create_default_node()

        @task
        def zk_get_children(self):
            with LocustTimer('get_children2') as ctx:
                c, stat = self._k.get_children('/kl', include_data=True)
                ctx.success(len(c))
