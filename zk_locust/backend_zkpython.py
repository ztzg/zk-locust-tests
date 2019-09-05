# ZKClient backend (libzookeeper_mt.so)

import threading

import zookeeper

from .backend_base import ZKLocustException, AbstractZKLocustClient

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

    def create(self, path, data="", flags=0, acl=None):
        if not acl:
            acl = [ZOO_OPEN_ACL_UNSAFE]
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

    def acreate(self, path, callback, data="", flags=0, acl=None):
        if not acl:
            acl = [ZOO_OPEN_ACL_UNSAFE]
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
    def __init__(self, hosts, **kwargs):
        super(ZKLocustClient, self).__init__(**kwargs)

        self._set_zk_client(ZKClient(hosts))
        # Messy.
        self.ensure_pseudo_root()

    def node_exists_except(self):
        return zookeeper.NodeExistsException

    def stop(self):
        self.get_zk_client().close()

    def create_default_node(self):
        path = self.join_path('/d-')
        flags = zookeeper.EPHEMERAL | zookeeper.SEQUENCE

        return self.get_zk_client().create(path, flags=flags)


ZKPYTHON_EXCEPTIONS = [ZKClientError]
