from locust import task

from common import ZKLocust, ZKLocustTaskSet, LocustTimer


class GetChildren(ZKLocust):

    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(GetChildren.task_set, self).__init__(parent)

            self._k = self.client.get_zk_client()
            self._n = self.client.create_default_node()

        @task
        def zk_get_children(self):
            with LocustTimer('get_children') as ctx:
                c = self._k.get_children('/kl')
                ctx.success(len(c))
