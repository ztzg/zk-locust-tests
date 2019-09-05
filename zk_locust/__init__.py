import time
import re
import os

from locust import Locust, TaskSet, events

from .backend_base import ZKLocustException

CLIENT_IMPL = os.getenv('ZK_LOCUST_CLIENT', 'kazoo')
ZK_HOSTS = os.getenv('ZK_LOCUST_HOSTS') or os.getenv('KAZOO_LOCUST_HOSTS')
PSEUDO_ROOT = os.getenv('ZK_LOCUST_PSEUDO_ROOT') or \
    os.getenv('KAZOO_LOCUST_PSEUDO_ROOT') or '/kl'
MIN_WAIT = int(os.getenv('ZK_LOCUST_MIN_WAIT', '0'))
MAX_WAIT = max(int(os.getenv('ZK_LOCUST_MAX_WAIT', '0')), MIN_WAIT)

_backend_exceptions_dict = {}
_backend_exceptions = ()


def get_zk_hosts():
    return ZK_HOSTS


def split_zk_hosts(raw):
    no_chroot = re.sub(r"/.*", "", raw.strip())
    return re.split(r"\s*,\s*", no_chroot)


def register_exceptions(exceptions):
    global _backend_exceptions
    for exception in exceptions:
        _backend_exceptions_dict[exception] = True
    _backend_exceptions = tuple(_backend_exceptions_dict.keys())


class ZKLocust(Locust):
    min_wait = MIN_WAIT
    max_wait = MAX_WAIT

    def __init__(self, client_impl=CLIENT_IMPL):
        super(ZKLocust, self).__init__()

        hosts = get_zk_hosts()
        pseudo_root = PSEUDO_ROOT

        if client_impl == 'kazoo':
            from .backend_kazoo import KazooLocustClient, KAZOO_EXCEPTIONS
            self.client = KazooLocustClient(
                hosts=hosts, pseudo_root=pseudo_root)
            register_exceptions(KAZOO_EXCEPTIONS)
        elif client_impl == 'zkpython':
            from .backend_zkpython import ZKLocustClient, ZKPYTHON_EXCEPTIONS
            self.client = ZKLocustClient(hosts=hosts, pseudo_root=pseudo_root)
            register_exceptions(ZKPYTHON_EXCEPTIONS)
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
            if isinstance(value, _backend_exceptions):
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
