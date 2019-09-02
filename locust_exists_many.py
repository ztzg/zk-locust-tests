from common import ZKLocust, LocustTimer

from locust import TaskSet, task


class ExistsMany(ZKLocust):

    class task_set(TaskSet):
        def __init__(self, parent):
            super(ExistsMany.task_set, self).__init__(parent)

            self._k = self.client.get_zk_client()
            self._i = 0

        @task
        def zk_exists_negative_watch(self):
            def zk_watch_trigger(event):
                pass

            with LocustTimer('exists_negative_watch') as ctx:
                self._i += 1
                self._k.exists('/kl/doesnotexist-' + str(self._i),
                               watch=zk_watch_trigger)
                ctx.success()
