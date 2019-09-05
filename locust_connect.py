import sys
import os

from locust import task

sys.path.append(os.getcwd())  # See "Common libraries" in Locust docs.
from zk_locust import ZKLocust, ZKLocustTaskSet, LocustTimer


class Connect(ZKLocust):
    def __init__(self):
        super(Connect, self).__init__(pseudo_root=None, autostart=False)

    class task_set(ZKLocustTaskSet):
        @task(10)
        def connect(self):
            try:
                with LocustTimer('connect') as ctx:
                    self.client.start()
                    ctx.success()
            finally:
                self.client.stop()
