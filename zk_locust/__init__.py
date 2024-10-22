import time
import re
import os

from enum import Enum

from locust import Locust, TaskSet, TaskSequence, events

from .backend_base import ZKLocustException

CLIENT_IMPL = os.getenv('ZK_LOCUST_CLIENT', 'kazoo')
ZK_HOSTS = os.getenv('ZK_LOCUST_HOSTS') or os.getenv('KAZOO_LOCUST_HOSTS')
PSEUDO_ROOT = os.getenv('ZK_LOCUST_PSEUDO_ROOT') or \
    os.getenv('KAZOO_LOCUST_PSEUDO_ROOT') or '/kl'
MIN_WAIT = int(os.getenv('ZK_LOCUST_MIN_WAIT', '0'))
MAX_WAIT = max(int(os.getenv('ZK_LOCUST_MAX_WAIT', '0')), MIN_WAIT)


class ExcBehavior(Enum):
    LOG_FAILURE = 0
    TRY_SUPPRESS = 1
    PROPAGATE = 2


_default_exc_behavior = ExcBehavior[os.getenv(
    'ZK_LOCUST_EXCEPTION_BEHAVIOR',
    ExcBehavior.LOG_FAILURE.name).upper().replace('-', '_')]

_backend_exceptions_set = set()
_backend_exceptions = ()
_backend_exceptions_non_suppress_set = set()
_backend_exceptions_non_suppress = ()

_zk_re_port = re.compile(r"(.*):(\d{1,4})$")


def get_zk_hosts():
    if not ZK_HOSTS:
        raise ZKLocustException("No ZK_LOCUST_HOSTS connect string provided")
    return ZK_HOSTS


def split_zk_hosts(connect_string=None):
    if not connect_string:
        connect_string = get_zk_hosts()

    no_chroot = re.sub(r"/.*", "", connect_string.strip())
    hosts = re.split(r"\s*,\s*", no_chroot)

    return hosts


def split_zk_host_port(host_port):
    r = _zk_re_port.match(host_port)
    if not r:
        return (host_port, None)
    else:
        return (r[1], int(r[2]))


def _add_backend_exceptions(exceptions, non_suppress=None):
    global _backend_exceptions, _backend_exceptions_non_suppress
    _backend_exceptions_set.update(exceptions)
    _backend_exceptions = tuple(_backend_exceptions_set)
    if non_suppress:
        _backend_exceptions_non_suppress_set.update(non_suppress)
        _backend_exceptions_non_suppress = tuple(
            _backend_exceptions_non_suppress_set)


def get_backend_exceptions():
    return _backend_exceptions


def note_backend_exception(exc_instance,
                           *,
                           request_type=None,
                           name=None,
                           exc_behavior=_default_exc_behavior):
    handled = False

    if exc_behavior is ExcBehavior.LOG_FAILURE:
        events.request_failure.fire(
            request_type=request_type or 'unknown',
            name=name or 'unknown',
            response_time=0,
            exception=exc_instance)
        handled = True
    elif exc_behavior is ExcBehavior.TRY_SUPPRESS:
        can_suppress = isinstance(exc_instance,
                                  _backend_exceptions_non_suppress)
        handled = not can_suppress

    return handled


class ZKLocust(Locust):
    min_wait = MIN_WAIT
    max_wait = MAX_WAIT

    def __init__(self,
                 client_impl=CLIENT_IMPL,
                 pseudo_root=PSEUDO_ROOT,
                 **kwargs):
        super(ZKLocust, self).__init__()

        hosts = get_zk_hosts()

        if client_impl == 'kazoo':
            from .backend_kazoo import KazooLocustClient, KAZOO_EXCEPTIONS, KAZOO_NON_SUPPRESS_EXCEPTIONS
            _add_backend_exceptions(KAZOO_EXCEPTIONS,
                                    KAZOO_NON_SUPPRESS_EXCEPTIONS)
            try:
                self.client = KazooLocustClient(
                    hosts=hosts, pseudo_root=pseudo_root, **kwargs)
            except KAZOO_EXCEPTIONS as e:
                note_backend_exception(e, name='backend_kazoo')
                raise  # Ignoring is not an option here
        elif client_impl == 'zkpython':
            from .backend_zkpython import ZKLocustClient, ZKPYTHON_EXCEPTIONS
            _add_backend_exceptions(ZKPYTHON_EXCEPTIONS)
            try:
                self.client = ZKLocustClient(
                    hosts=hosts, pseudo_root=pseudo_root, **kwargs)
            except ZKPYTHON_EXCEPTIONS as e:
                note_backend_exception(e, name='backend_zkpython')
                raise  # Ignoring is not an option here
        else:
            raise ZKLocustException(
                "Unknown value for 'client_impl': %s" % (client_impl))

    def stop(self):
        self.client.stop()


class ZKLocustTaskSet(TaskSet):
    def __init__(self, parent, maybe_interrupt=None, *args, **kwargs):
        super(ZKLocustTaskSet, self).__init__(parent, *args, **kwargs)

        self.maybe_interrupt = maybe_interrupt

    def on_stop(self):
        # super?
        if isinstance(self.parent, ZKLocust):
            self.parent.stop()


class ZKLocustTaskSequence(TaskSequence):
    def __init__(self, parent, maybe_interrupt=None, *args, **kwargs):
        super(ZKLocustTaskSequence, self).__init__(parent, *args, **kwargs)

        self.maybe_interrupt = maybe_interrupt

    def on_stop(self):
        # super?
        if isinstance(self.parent, ZKLocust):
            self.parent.stop()


class LocustTimer(object):
    _start_time = None
    _is_reported = False

    def __init__(self,
                 request_type,
                 name='',
                 *,
                 exc_behavior=_default_exc_behavior):
        self._request_type = request_type
        self._name = name
        self._exc_behavior = exc_behavior

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, exc, value, traceback):
        if self._is_reported:
            # if the user has already manually marked this response as
            # failure or success we can ignore the default haviour of
            # letting the response code determine the outcome
            return exc is None

        handled = False

        if exc:
            if isinstance(value, _backend_exceptions):
                if self._exc_behavior is ExcBehavior.LOG_FAILURE:
                    self.failure(value)
                    handled = True
                else:
                    handled = self._exc_behavior is ExcBehavior.TRY_SUPPRESS
            else:
                handled = False
        else:
            self.success()
            handled = True

        return handled

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
