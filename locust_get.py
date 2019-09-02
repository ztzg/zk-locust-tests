import random

from locust import TaskSet, task

from common import ZKLocust, LocustTimer

import zookeeper

import kazoo.exceptions


key_size = 8
val_size = 8
# rate = 0
# total = 10000
key_space_size = 128
sequential_keys = False
# check_hashkv = False


key_seq = 0
# Note: zkpython does not support binary values!
v = bytearray(random.randint(32, 127) for _ in range(val_size))


class Get(ZKLocust):

    class task_set(TaskSet):
        def __init__(self, parent):
            super(Get.task_set, self).__init__(parent)

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
            except (zookeeper.NodeExistsException,
                    kazoo.exceptions.NodeExistsError):
                pass

            self._n = n

        @task(10)
        def zk_get(self):
            with LocustTimer('get') as ctx:
                self._k.get(self._n)
                ctx.success()
