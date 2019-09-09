import random
import os

from datetime import datetime

from gevent import GreenletExit

from locust import Locust, TaskSet

from . import LocustTimer

_default_key_size = int(os.getenv('ZK_LOCUST_KEY_SIZE', '8'))
_default_val_size = int(os.getenv('ZK_LOCUST_VAL_SIZE', '8'))

key_seq = 0


def interrupt(task_set):
    if isinstance(task_set.parent, TaskSet):
        task_set.interrupt(reschedule=False)
    elif isinstance(task_set.parent, Locust):
        raise GreenletExit()


def iterations(count):
    n = 0

    def iteration(task_set):
        nonlocal n
        if n >= count:
            interrupt(task_set)
        n += 1

    return iteration


def duration(delta):
    deadline = datetime.now() + delta

    def iteration(task_set):
        if datetime.now() > deadline:
            interrupt(task_set)

    return iteration


class ZKSimpleGet(object):
    def __init__(self,
                 client,
                 maybe_interrupt=None,
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=_default_val_size):
        self._client = client
        self._maybe_interrupt = maybe_interrupt
        self._k = self._client.get_zk_client()

        if (sequential_keys):
            global key_seq
            key_i = key_seq % key_space_size
            key_seq += 1
        else:
            key_i = random.randrange(0, key_space_size)

        n = self._client.join_path('/c-' + str(key_i).zfill(key_size - 2))
        # Note: zkpython does not support binary values!
        v = bytes(random.randint(32, 127) for _ in range(val_size))

        try:
            self._k.create(n, v)
        except self._client.node_exists_except():
            pass

        self._n = n

    def task(self, task_set):
        if self._maybe_interrupt is not None:
            self._maybe_interrupt(task_set)

        with LocustTimer('get') as ctx:
            self._k.get(self._n)
            ctx.success()
