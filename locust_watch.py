import time
import sys

from locust import TaskSet, task, events

from common import KazooLocust


val_size = 8


class Watch(KazooLocust):
    min_wait = 0
    max_wait = 100

    class task_set(TaskSet):
        def __init__(self, parent):
            super(Watch.task_set, self).__init__(parent)

            self._k = self.client.get_kazoo_client()
            self._n = self.client.create_default_node()

        @task
        def zk_watch(self):
            def zk_watch_trigger(event):
                end_time = time.time()
                v, stat = self._k.get(self._n)
                # Decode start_time from payload
                start_time = int.from_bytes(v, byteorder=sys.byteorder) / 1000

                events.request_success.fire(
                    request_type='watch',
                    name='',
                    response_time=int((end_time - start_time) * 1000),
                    response_length=0,
                )

            self._k.get(self._n, watch=zk_watch_trigger)
            # Encode start_time as payload.
            v = int(time.time() * 1000).to_bytes(val_size,
                                                 byteorder=sys.byteorder)
            self._k.set_async(self._n, v)
