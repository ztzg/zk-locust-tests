import random

from locust import TaskSet, task

import kazoo

from common import KazooLocust, LocustTimer


key_size = 8
val_size = 8
# rate = 0
# total = 10000
key_space_size = 128
sequential_keys = False
# check_hashkv = False


key_seq = 0


class SetAndGet(KazooLocust):

    min_wait = 0
    max_wait = 0

    class task_set(TaskSet):
        _i = 0

        def __init__(self, parent):
            super(SetAndGet.task_set, self).__init__(parent)

            self._k = self.client.get_kazoo_client()

            if (sequential_keys):
                global key_seq
                key_i = key_seq % key_space_size
                key_seq += 1
            else:
                key_i = random.randrange(0, key_space_size)

            n = self.client.join_path('/c-' + str(key_i).zfill(key_size - 2))

            acl = None
            if (self.client.has_sasl_auth()):
                # "auth"
                acl = kazoo.security.CREATOR_ALL_ACL

            try:
                self._k.create(n, self.next_val(), acl=acl)
            except kazoo.exceptions.NodeExistsError:
                pass

            self._n = n

        def next_val(self):
            v = str(self._i).zfill(val_size).encode('ascii')
            self._i += 1
            return v

        @task(1)
        def zk_set(self):
            v = self.next_val()

            with LocustTimer('set') as ctx:
                self._k.set(self._n, v)
                ctx.success()

        @task(10)
        def zk_get(self):
            with LocustTimer('get') as ctx:
                v = self._k.get(self._n)
                ctx.success(response_length=len(v))
