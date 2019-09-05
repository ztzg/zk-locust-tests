import time
import os
import json
import threading

from locust import Locust, TaskSet, events

import zookeeper

import kazoo.handlers.gevent
import kazoo.handlers.threading
import kazoo.client
import kazoo.exceptions

_default_client = os.getenv('ZK_LOCUST_CLIENT', 'kazoo')
_default_hosts = os.getenv('ZK_LOCUST_HOSTS') or \
    os.getenv('KAZOO_LOCUST_HOSTS')
_default_pseudo_root = os.getenv('ZK_LOCUST_PSEUDO_ROOT') or \
    os.getenv('KAZOO_LOCUST_PSEUDO_ROOT') or '/kl'
_default_min_wait = int(os.getenv('ZK_LOCUST_MIN_WAIT', '0'))
_default_max_wait = max(
    int(os.getenv('ZK_LOCUST_MAX_WAIT', '0')), _default_min_wait)


class ZKLocustException(Exception):
    pass


class AbstractZKLocustClient(object):
    _pseudo_root = None
    _zk_client = None

    def __init__(self, pseudo_root=_default_pseudo_root):
        self._pseudo_root = pseudo_root

    def _set_zk_client(self, client):
        self._zk_client = client

    def get_zk_client(self):
        return self._zk_client

    def has_sasl_auth(self):
        return False

    def ensure_pseudo_root(self):
        if self._pseudo_root:
            try:
                self.get_zk_client().create(self._pseudo_root)
            except (zookeeper.NodeExistsException,
                    kazoo.exceptions.NodeExistsError):
                pass

    def join_path(self, path):
        if self._pseudo_root:
            return self._pseudo_root + path
        else:
            return path


# ZKClient backend (libzookeeper_mt.so)

ZK_DEFAULT_TIMEOUT = 30000
ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}


class ZKClientError(ZKLocustException):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


# Based on ZKClient from https://github.com/phunt/zk-smoketest
class ZKClient(object):
    def __init__(self, servers, timeout=ZK_DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.connected = False
        self.conn_cv = threading.Condition()
        self.handle = -1

        self.conn_cv.acquire()
        zookeeper.init(servers, self.connection_watcher, timeout)
        self.conn_cv.wait(timeout / 1000)
        self.conn_cv.release()

        if not self.connected:
            raise ZKClientError("Unable to connect to %s" % (servers))

    def connection_watcher(self, h, type, state, path):
        self.handle = h
        self.conn_cv.acquire()
        self.connected = True
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def close(self):
        return zookeeper.close(self.handle)

    def create(self, path, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        return zookeeper.create(self.handle, path, data, acl, flags)

    def delete(self, path, version=-1):
        return zookeeper.delete(self.handle, path, version)

    def get(self, path, watcher=None):
        return zookeeper.get(self.handle, path, watcher)

    def exists(self, path, watcher=None):
        return zookeeper.exists(self.handle, path, watcher)

    def set(self, path, data="", version=-1):
        return zookeeper.set(self.handle, path, data, version)

    def set2(self, path, data="", version=-1):
        return zookeeper.set2(self.handle, path, data, version)

    def get_children(self, path, watcher=None):
        return zookeeper.get_children(self.handle, path, watcher)

    # def async(self, path="/"):
    #     return zookeeper.async(self.handle, path)

    def acreate(self,
                path,
                callback,
                data="",
                flags=0,
                acl=[ZOO_OPEN_ACL_UNSAFE]):
        return zookeeper.acreate(self.handle, path, data, acl, flags, callback)

    def adelete(self, path, callback, version=-1):
        return zookeeper.adelete(self.handle, path, version, callback)

    def aget(self, path, callback, watcher=None):
        return zookeeper.aget(self.handle, path, watcher, callback)

    def aexists(self, path, callback, watcher=None):
        return zookeeper.aexists(self.handle, path, watcher, callback)

    def aset(self, path, callback, data="", version=-1):
        return zookeeper.aset(self.handle, path, data, version, callback)


class ZKLocustClient(AbstractZKLocustClient):
    def __init__(self, hosts=_default_hosts, **kwargs):
        super(ZKLocustClient, self).__init__(**kwargs)

        self._set_zk_client(ZKClient(hosts))
        # Messy.
        self.ensure_pseudo_root()

    def stop(self):
        self.get_zk_client().close()

    def create_default_node(self):
        path = self.join_path('/d-')
        flags = zookeeper.EPHEMERAL | zookeeper.SEQUENCE

        return self.get_zk_client().create(path, flags=flags)


# Kazoo backend


class KazooLocustException(ZKLocustException):
    pass


class KazooLocustNoHostsException(KazooLocustException):
    pass


class KazooLocustArgumentsException(KazooLocustException):
    pass


class KazooLocustStartedException(KazooLocustException):
    pass


class KazooLocustStoppedException(KazooLocustException):
    pass


_default_handler = os.getenv('KAZOO_LOCUST_HANDLER')


def fetch_default_sasl_options():
    s = os.getenv('KAZOO_LOCUST_SASL_OPTIONS')
    if not s:
        return None
    return json.loads(s)


_default_sasl_options = fetch_default_sasl_options()


class KazooLocustClient(AbstractZKLocustClient):
    _started = False
    _sasl_options = None
    _pseudo_root = None

    def __init__(self,
                 hosts=_default_hosts,
                 handler=_default_handler,
                 sasl_options=_default_sasl_options,
                 pseudo_root=_default_pseudo_root,
                 autostart=True):
        super(KazooLocustClient, self).__init__(pseudo_root=pseudo_root)

        if not hosts:
            raise KazooLocustNoHostsException()

        # Avoid passing unknown sasl_options to "old" Kazoo.
        kwargs = {}
        if sasl_options:
            kwargs['sasl_options'] = sasl_options

        if handler:
            if handler == 'gevent':
                inst = kazoo.handlers.gevent.SequentialGeventHandler()
            elif handler == 'threading':
                inst = kazoo.handlers.threading.SequentialThreadingHandler()
            else:
                raise KazooLocustArgumentsException(
                    "Unknown value for 'handler': %s" % (handler))
            kwargs['handler'] = inst

        self._set_zk_client(kazoo.client.KazooClient(hosts=hosts, **kwargs))

        self._sasl_options = sasl_options

        if autostart:
            self.start()

    def start(self):
        if self._started:
            raise KazooLocustStartedException()
        super(KazooLocustClient, self).get_zk_client().start()
        self._started = True
        # Messy.
        self.ensure_pseudo_root()

    def stop(self):
        self.get_zk_client().stop()
        self._started = False

    def has_sasl_auth(self):
        return self._sasl_options is not None

    def get_zk_client(self):
        if self._started:
            return super(KazooLocustClient, self).get_zk_client()
        raise KazooLocustStoppedException()

    def create_default_node(self):
        path = self.join_path('/d-')
        client = self.get_zk_client()

        return client.create(path, ephemeral=True, sequence=True)


class ZKLocust(Locust):

    min_wait = _default_min_wait
    max_wait = _default_max_wait

    def __init__(self, client_impl=_default_client):
        super(ZKLocust, self).__init__()

        if client_impl == 'kazoo':
            self.client = KazooLocustClient()
        elif client_impl == 'zkpython':
            self.client = ZKLocustClient()
        else:
            raise ZKLocustException(
                "Unknown value for 'client_impl': %s" % (client_impl))

    def stop(self):
        self.client.stop()


class ZKLocustTaskSet(TaskSet):
    def on_stop(self):
        # super?
        if isinstance(self.parent, ZKLocust):
            self.parent.stop()


class LocustTimer(object):
    _request_type = None
    _name = None
    _start_time = None
    _is_reported = False

    def __init__(self, request_type, name=''):
        self._request_type = request_type
        self._name = name

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, exc, value, traceback):
        if self._is_reported:
            # if the user has already manually marked this response as
            # failure or success we can ignore the default haviour of
            # letting the response code determine the outcome
            return exc is None

        if exc:
            if isinstance(value, kazoo.exceptions.KazooException):
                self.failure(value)
            else:
                return False
        else:
            self.success()
        return True

    def success(self, response_length=0):
        """
        Report the response as successful
        """
        events.request_success.fire(
            request_type=self._request_type,
            name=self._name,
            response_time=int((time.time() - self._start_time) * 1000),
            response_length=response_length,
        )
        self._is_reported = True

    def failure(self, exc):
        """
        Report the response as a failure.

        exc can be either a python exception, or a string in which case it will
        be wrapped inside a CatchResponseError.
        """
        events.request_failure.fire(
            request_type=self._request_type,
            name=self._name,
            response_time=int((time.time() - self._start_time) * 1000),
            exception=exc,
        )
        self._is_reported = True
