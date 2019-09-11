import random
import os
import sys
import time

from datetime import datetime

from gevent import GreenletExit

from locust import Locust, TaskSet, events

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


def _gen_test_path(client, key_size, sequential_keys, key_space_size):
    if (sequential_keys):
        global key_seq
        key_i = key_seq % key_space_size
        key_seq += 1
    else:
        key_i = random.randrange(0, key_space_size)

    return client.join_path('/c-' + str(key_i).zfill(key_size - 2))


def _gen_random_bytes(val_size):
    # Note: zkpython does not support binary values!
    return bytes(random.randint(32, 127) for _ in range(val_size))


def _create_random_key(client, key_size, sequential_keys, key_space_size,
                       val_size):
    k = client.get_zk_client()
    n = _gen_test_path(client, key_size, sequential_keys, key_space_size)
    v = _gen_random_bytes(val_size)

    try:
        k.create(n, v)
    except client.node_exists_except():
        pass

    return (n, v)


class AbstractOp(object):
    def __init__(self, client, maybe_interrupt=None):
        self.client = client
        self.maybe_interrupt = maybe_interrupt

    def task(self, task_set):
        if self.maybe_interrupt is not None:
            self.maybe_interrupt(task_set)
        self.op()


class ZKConnectOp(AbstractOp):
    def __init__(self, client, maybe_interrupt=None):
        super(ZKConnectOp, self).__init__(client, maybe_interrupt)

    def op(self):
        try:
            with LocustTimer('connect') as ctx:
                self.client.start()
                ctx.success()
        finally:
            self.client.stop()


class ZKGetOp(AbstractOp):
    def __init__(self,
                 client,
                 maybe_interrupt=None,
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=_default_val_size,
                 label='get'):
        super(ZKGetOp, self).__init__(client, maybe_interrupt)
        self._k = self.client.get_zk_client()

        n, v = _create_random_key(client, key_size, sequential_keys,
                                  key_space_size, val_size)

        self._n = n
        self._label = label

    def op(self):
        with LocustTimer(self._label) as ctx:
            self._k.get(self._n)
            ctx.success()


class ZKSetOp(AbstractOp):
    def __init__(self,
                 client,
                 maybe_interrupt=None,
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=_default_val_size):
        super(ZKSetOp, self).__init__(client, maybe_interrupt)
        self._k = self.client.get_zk_client()

        n, v = _create_random_key(client, key_size, sequential_keys,
                                  key_space_size, val_size)

        self._n = n
        self._v = v

    def op(self):
        with LocustTimer('set') as ctx:
            self._k.set(self._n, self._v)
            ctx.success()


class ZKIncrementingSetOp(AbstractOp):
    def __init__(self,
                 client,
                 maybe_interrupt=None,
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=_default_val_size):
        super(ZKIncrementingSetOp, self).__init__(client, maybe_interrupt)
        self._k = self.client.get_zk_client()

        n = _gen_test_path(client, key_size, sequential_keys, key_space_size)

        self._n = n
        self._i = 0
        self._val_size = val_size

        v = self.next_val()

        try:
            self._k.create(n, v)
        except client.node_exists_except():
            pass

    def next_val(self):
        v = str(self._i).zfill(self._val_size).encode('ascii')
        self._i += 1
        return v

    def op(self):
        v = self.next_val()

        with LocustTimer('incr_set') as ctx:
            self._k.set(self._n, v)
            ctx.success()


class ZKCreateEphemeralOp(AbstractOp):
    def __init__(self, client, maybe_interrupt=None, base_path=None,
                 push=None):
        super(ZKCreateEphemeralOp, self).__init__(client, maybe_interrupt)

        if not base_path:
            base_path = client.join_path('/c-')

        self._k = client.get_zk_client()
        self._base_path = base_path
        self._push = push

    def op(self):
        k = None
        with LocustTimer('create_ephemeral') as ctx:
            k = self._k.create(self._base_path, ephemeral=True, sequence=True)
            ctx.success()
        if k and self._push:
            self._push(k)


class ZKDeleteFromQueueOp(AbstractOp):
    def __init__(self, client, pop, maybe_interrupt=None):
        super(ZKDeleteFromQueueOp, self).__init__(client, maybe_interrupt)

        self._k = client.get_zk_client()
        self._pop = pop

    def op(self):
        k = None
        try:
            k = self._pop()
        except IndexError:
            pass

        if k:
            with LocustTimer('delete') as ctx:
                self._k.delete(k)
                ctx.success()


class ZKCountChildrenOp(AbstractOp):
    def __init__(self, client, maybe_interrupt=None, path=None):
        super(ZKCountChildrenOp, self).__init__(client, maybe_interrupt)

        if not path:
            path = client.join_path('/')

        self._k = client.get_zk_client()
        self._path = path

    def op(self):
        with LocustTimer('count_children') as ctx:
            s = self._k.exists(self._path)
            ctx.success(response_length=s.children_count)


class ZKExistsOp(AbstractOp):
    def __init__(self, client, path, label, maybe_interrupt=None):
        super(ZKExistsOp, self).__init__(client, maybe_interrupt)

        self._k = client.get_zk_client()
        self._path = path
        self._label = label

    def op(self):
        with LocustTimer(self._label) as ctx:
            # Answer is ignored.
            self._k.exists(self._path)
            ctx.success()


class ZKExistsWithWatchOp(AbstractOp):
    def __init__(self, client, path, label, maybe_interrupt=None):
        super(ZKExistsWithWatchOp, self).__init__(client, maybe_interrupt)

        self._k = client.get_zk_client()
        self._path = path
        self._label = label

    def op(self):
        def zk_watch_trigger(event):
            pass

        with LocustTimer(self._label) as ctx:
            # Answer is ignored.
            self._k.exists(self._path, watch=zk_watch_trigger)
            ctx.success()


class ZKExistsWithManyWatchesOp(AbstractOp):
    def __init__(self, client, maybe_interrupt=None, base_path=None):
        super(ZKExistsWithManyWatchesOp, self).__init__(
            client, maybe_interrupt)

        if not base_path:
            base_path = client.join_path('/doesnotexist-')

        self._k = client.get_zk_client()
        self._base_path = base_path
        self._i = 0

    def op(self):
        def zk_watch_trigger(event):
            pass

        with LocustTimer('exists_negative_watch_many') as ctx:
            self._i += 1
            # Answer is ignored.
            self._k.exists(
                self._base_path + str(self._i), watch=zk_watch_trigger)
            ctx.success()


class ZKGetChildrenOp(AbstractOp):
    def __init__(self, client, path, maybe_interrupt=None):
        super(ZKGetChildrenOp, self).__init__(client, maybe_interrupt)

        self._k = self.client.get_zk_client()
        self._path = path

    def op(self):
        with LocustTimer('get_children') as ctx:
            c = self._k.get_children(self._path)
            ctx.success(len(c))


class ZKGetChildren2Op(AbstractOp):
    def __init__(self, client, path, maybe_interrupt=None):
        super(ZKGetChildren2Op, self).__init__(client, maybe_interrupt)

        self._k = self.client.get_zk_client()
        self._path = path

    def op(self):
        with LocustTimer('get_children2') as ctx:
            c, stat = self._k.get_children(self._path, include_data=True)
            ctx.success(len(c))


class ZKWatchOp(AbstractOp):
    def __init__(self,
                 client,
                 path,
                 maybe_interrupt=None,
                 val_size=_default_val_size):
        super(ZKWatchOp, self).__init__(client, maybe_interrupt)

        self._k = self.client.get_zk_client()
        self._path = path
        self._val_size = val_size

    def op(self):
        def zk_watch_trigger(event):
            end_time = time.time()
            v, stat = self._k.get(self._path)
            # Decode start_time from payload
            start_time = int.from_bytes(v, byteorder=sys.byteorder) / 1000

            events.request_success.fire(
                request_type='watch',
                name='',
                response_time=int((end_time - start_time) * 1000),
                response_length=0,
            )

        self._k.get(self._path, watch=zk_watch_trigger)
        # Encode start_time as payload.
        v = int(time.time() * 1000).to_bytes(
            self._val_size, byteorder=sys.byteorder)
        self._k.set_async(self._path, v)
