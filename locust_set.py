import os
import random

from locust import TaskSet, task

from kazoo.exceptions import NodeExistsError

from common import ZKLocust, LocustTimer


key_size = 8
val_size = 8
# rate = 0
# total = 10000
key_space_size = 128
sequential_keys = False
# check_hashkv = False


key_seq = 0
v = os.getrandom(val_size)


class Set(ZKLocust):

    class task_set(TaskSet):
        def __init__(self, parent):
            super(Set.task_set, self).__init__(parent)

            self._k = self.client.get_zk_client()

            if (sequential_keys):
                global key_seq
                key_i = key_seq % key_space_size
                key_seq += 1
            else:
                key_i = random.randrange(0, key_space_size)

            n = self.client.join_path('/c-' + str(key_i).zfill(key_size - 2))

            try:
                self._k.create(n, v)
            except NodeExistsError:
                pass

            self._n = n

        @task(10)
        def zk_set(self):
            with LocustTimer('set') as ctx:
                self._k.set(self._n, v)
                ctx.success()
