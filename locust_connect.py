from locust import TaskSet, task

from common import ZKLocust, LocustTimer


class Connect(ZKLocust):

    def __init__(self):
        super(Connect, self).__init__(pseudo_root=None,
                                      autostart=False)

    class task_set(TaskSet):

        @task(10)
        def connect(self):
            try:
                with LocustTimer('connect') as ctx:
                    self.client.start()
                    ctx.success()
            finally:
                self.client.stop()
