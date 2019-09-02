from locust import task

from common import ZKLocust, ZKLocustTaskSet, LocustTimer


class Exists(ZKLocust):

    class task_set(ZKLocustTaskSet):
        def __init__(self, parent):
            super(Exists.task_set, self).__init__(parent)

            self._k = self.client.get_zk_client()
            self._n = self.client.create_default_node()

        def teardown(self):
            self.client.stop()

        @task
        def zk_exists_positive(self):
            with LocustTimer('exists_positive') as ctx:
                self._k.exists(self._n)
                ctx.success()

        @task
        def zk_exists_negative(self):
            with LocustTimer('exists_negative') as ctx:
                self._k.exists('/kl/doesnotexist')
                ctx.success()

        @task
        def zk_exists_positive_watch(self):
            def zk_watch_trigger(event):
                pass

            with LocustTimer('exists_positive_watch') as ctx:
                self._k.exists(self._n, watch=zk_watch_trigger)
                ctx.success()

        @task
        def zk_exists_negative_watch(self):
            def zk_watch_trigger(event):
                pass

            with LocustTimer('exists_negative_watch') as ctx:
                self._k.exists('/kl/doesnotexist', watch=zk_watch_trigger)
                ctx.success()
