import sys
import os

from locust import task

sys.path.append(os.getcwd())  # See "Common libraries" in Locust docs.
from zk_locust import ZKLocust, ZKLocustTaskSet, LocustTimer


class GetChildren2(ZKLocust):
    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(GetChildren2.task_set, self).__init__(parent)

            self._k = self.client.get_zk_client()
            self._n = self.client.create_default_node()

        @task
        def zk_get_children(self):
            with LocustTimer('get_children2') as ctx:
                c, stat = self._k.get_children('/kl', include_data=True)
                ctx.success(len(c))
