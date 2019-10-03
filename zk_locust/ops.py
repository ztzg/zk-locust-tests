import random
import os
import sys
import time

from datetime import datetime

from gevent import GreenletExit

from locust import Locust, TaskSet, events

from . import LocustTimer

_default_key_size = int(os.getenv('ZK_LOCUST_KEY_SIZE') or '8')
_default_val_size = int(os.getenv('ZK_LOCUST_VAL_SIZE') or '8')

_default_ignore_connection_down = int(
    os.getenv('ZK_LOCUST_IGNORE_CONNECTION_DOWN') or '0') > 0

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
    if val_size is None:
        val_size = _default_val_size
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
    def __init__(self,
                 client,
                 *,
                 maybe_interrupt=None,
                 ignore_connection_down=None):
        self.client = client
        self._maybe_interrupt = maybe_interrupt
        self._task_set = None
        self._tick = None
        self._ignore_connection_down = _default_ignore_connection_down
        if ignore_connection_down is not None:
            self._ignore_connection_down = ignore_connection_down

    def task(self, task_set):
        if task_set is not self._task_set:
            self._task_set = task_set
            opmi = self._maybe_interrupt
            if hasattr(task_set, "maybe_interrupt"):
                tsmi = task_set.maybe_interrupt
            if tsmi and opmi:

                def both(task_set):
                    opmi(task_set)
                    tsmi(task_set)

                self._tick = both
            else:
                self._tick = opmi or tsmi
        if self._tick:
            self._tick(task_set)
        if self._ignore_connection_down and self.client.is_connection_down():
            return
        self.op()


class AbstractSingleTimerOp(AbstractOp):
    def __init__(self,
                 client,
                 *,
                 request_type=None,
                 task_set_name=None,
                 **kwargs):
        super(AbstractSingleTimerOp, self).__init__(client, **kwargs)
        self._request_type = request_type
        self._task_set_name = task_set_name

    def timing(self, request_type=None, task_set_name=None):
        return LocustTimer(
            request_type=request_type or self._request_type or '',
            name=task_set_name or self._task_set_name or '')


class ZKConnectOp(AbstractSingleTimerOp):
    def __init__(self, *args, **kwargs):
        super(ZKConnectOp, self).__init__(*args, **kwargs)

    def op(self):
        try:
            with self.timing() as ctx:
                self.client.start()
                ctx.success()
        finally:
            self.client.stop()


class ZKGetOp(AbstractSingleTimerOp):
    def __init__(self,
                 client,
                 *,
                 request_type='get',
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=None,
                 **kwargs):
        super(ZKGetOp, self).__init__(
            client, request_type=request_type, **kwargs)
        self._k = self.client.get_zk_client()

        n, v = _create_random_key(client, key_size, sequential_keys,
                                  key_space_size, val_size)

        self._n = n

    def op(self):
        with self.timing() as ctx:
            self._k.get(self._n)
            ctx.success()


class ZKSetOp(AbstractSingleTimerOp):
    def __init__(self,
                 client,
                 *,
                 request_type='set',
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=None,
                 **kwargs):
        super(ZKSetOp, self).__init__(
            client, request_type=request_type, **kwargs)
        self._k = self.client.get_zk_client()

        n, v = _create_random_key(client, key_size, sequential_keys,
                                  key_space_size, val_size)

        self._n = n
        self._v = v

    def op(self):
        with self.timing() as ctx:
            self._k.set(self._n, self._v)
            ctx.success()


class ZKIncrementingSetOp(AbstractSingleTimerOp):
    def __init__(self,
                 client,
                 *,
                 request_type='incr_set',
                 sequential_keys=False,
                 key_space_size=128,
                 key_size=_default_key_size,
                 val_size=None,
                 **kwargs):
        super(ZKIncrementingSetOp, self).__init__(
            client, request_type=request_type, **kwargs)
        self._k = self.client.get_zk_client()

        n = _gen_test_path(client, key_size, sequential_keys, key_space_size)

        self._n = n
        self._i = 0
        self._val_size = val_size or _default_val_size

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

        with self.timing() as ctx:
            self._k.set(self._n, v)
            ctx.success()


class ZKCreateEphemeralOp(AbstractSingleTimerOp):
    def __init__(self,
                 client,
                 *,
                 request_type='create_ephemeral',
                 base_path=None,
                 val_size=None,
                 push=None,
                 **kwargs):
        super(ZKCreateEphemeralOp, self).__init__(
            client, request_type=request_type, **kwargs)

        if not base_path:
            base_path = client.join_path('/c-')

        self._k = client.get_zk_client()
        self._base_path = base_path
        self._v = _gen_random_bytes(val_size)
        self._push = push

    def op(self):
        k = None
        with self.timing() as ctx:
            k = self._k.create(
                self._base_path, self._v, ephemeral=True, sequence=True)
            ctx.success()
        if k and self._push:
            self._push(k)


class ZKDeleteFromQueueOp(AbstractSingleTimerOp):
    def __init__(self, client, pop, *, request_type='delete', **kwargs):
        super(ZKDeleteFromQueueOp, self).__init__(
            client, request_type=request_type, **kwargs)

        self._k = client.get_zk_client()
        self._pop = pop

    def op(self):
        k = None
        try:
            k = self._pop()
        except IndexError:
            pass

        if k:
            with self.timing() as ctx:
                self._k.delete(k)
                ctx.success()


class ZKCountChildrenOp(AbstractSingleTimerOp):
    def __init__(self,
                 client,
                 *,
                 request_type='count_children',
                 path=None,
                 **kwargs):
        super(ZKCountChildrenOp, self).__init__(
            client, request_type=request_type, **kwargs)

        if not path:
            path = client.join_path('/')

        self._k = client.get_zk_client()
        self._path = path

    def op(self):
        with self.timing() as ctx:
            s = self._k.exists(self._path)
            ctx.success(response_length=s.children_count)


class ZKExistsOp(AbstractSingleTimerOp):
    def __init__(self, client, path, *, request_type='exists', **kwargs):
        super(ZKExistsOp, self).__init__(
            client, request_type=request_type, **kwargs)

        self._k = client.get_zk_client()
        self._path = path

    def op(self):
        with self.timing() as ctx:
            # Answer is ignored.
            self._k.exists(self._path)
            ctx.success()


class ZKExistsWithWatchOp(AbstractSingleTimerOp):
    def __init__(self, client, path, *, request_type='exists_watch', **kwargs):
        super(ZKExistsWithWatchOp, self).__init__(
            client, request_type=request_type, **kwargs)

        self._k = client.get_zk_client()
        self._path = path

    def op(self):
        def zk_watch_trigger(event):
            pass

        with self.timing() as ctx:
            # Answer is ignored.
            self._k.exists(self._path, watch=zk_watch_trigger)
            ctx.success()


class ZKExistsWithManyWatchesOp(AbstractSingleTimerOp):
    def __init__(self,
                 client,
                 *,
                 request_type='exists_negative_watch_many',
                 base_path=None,
                 **kwargs):
        super(ZKExistsWithManyWatchesOp, self).__init__(
            client, request_type=request_type, **kwargs)

        if not base_path:
            base_path = client.join_path('/doesnotexist-')

        self._k = client.get_zk_client()
        self._base_path = base_path
        self._i = 0

    def op(self):
        def zk_watch_trigger(event):
            pass

        with self.timing() as ctx:
            self._i += 1
            # Answer is ignored.
            self._k.exists(
                self._base_path + str(self._i), watch=zk_watch_trigger)
            ctx.success()


class ZKGetChildrenOp(AbstractSingleTimerOp):
    def __init__(self, client, path, *, request_type='get_children', **kwargs):
        super(ZKGetChildrenOp, self).__init__(
            client, request_type=request_type, **kwargs)

        self._k = self.client.get_zk_client()
        self._path = path

    def op(self):
        with self.timing() as ctx:
            c = self._k.get_children(self._path)
            ctx.success(len(c))


class ZKGetChildren2Op(AbstractSingleTimerOp):
    def __init__(self, client, path, *, request_type='get_children2',
                 **kwargs):
        super(ZKGetChildren2Op, self).__init__(
            client, request_type=request_type, **kwargs)

        self._k = self.client.get_zk_client()
        self._path = path

    def op(self):
        with self.timing() as ctx:
            c, stat = self._k.get_children(self._path, include_data=True)
            ctx.success(len(c))


class ZKWatchOp(AbstractOp):
    def __init__(self,
                 client,
                 path,
                 *,
                 request_type='watch',
                 task_set_name='',
                 val_size=None,
                 **kwargs):
        super(ZKWatchOp, self).__init__(client, **kwargs)

        self._k = self.client.get_zk_client()
        self._request_type = request_type
        self._task_set_name = task_set_name
        self._path = path
        self._val_size = val_size or _default_val_size

    def op(self):
        def zk_watch_trigger(event):
            end_time = time.time()
            v, stat = self._k.get(self._path)
            # Decode start_time from payload
            start_time = int.from_bytes(v, byteorder=sys.byteorder) / 1000

            events.request_success.fire(
                request_type=self._request_type,
                name=self._task_set_name,
                response_time=int((end_time - start_time) * 1000),
                response_length=0,
            )

        self._k.get(self._path, watch=zk_watch_trigger)
        # Encode start_time as payload.
        v = int(time.time() * 1000).to_bytes(
            self._val_size, byteorder=sys.byteorder)
        self._k.set_async(self._path, v)
