import sys
import os

from locust import task

sys.path.append(os.getcwd())  # See "Common libraries" in Locust docs.
from zk_locust import ZKLocust, ZKLocustTaskSet, LocustTimer


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
