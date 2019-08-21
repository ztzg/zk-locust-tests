import time
import os
import json

from locust import Locust, events

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NodeExistsError


class KazooLocustException(Exception):
    pass


class KazooLocustNoHostsProvided(KazooLocustException):
    pass


class KazooLocustStarted(KazooLocustException):
    pass


class KazooLocustStopped(KazooLocustException):
    pass


_default_hosts = os.getenv('KAZOO_LOCUST_HOSTS')
_default_pseudo_root = os.getenv('KAZOO_LOCUST_PSEUDO_ROOT', '/kl')


def fetch_default_sasl_options():
    s = os.getenv('KAZOO_LOCUST_SASL_OPTIONS')
    if not s:
        return None
    return json.loads(s)


_default_sasl_options = fetch_default_sasl_options()


class KazooLocustClient(object):
    _client = None
    _started = False
    _sasl_options = None
    _pseudo_root = None

    def __init__(self,
                 hosts=_default_hosts,
                 sasl_options=_default_sasl_options,
                 pseudo_root=_default_pseudo_root,
                 autostart=True):
        if not hosts:
            raise KazooLocustNoHostsProvided()

        self._client = KazooClient(
            hosts=hosts,
            sasl_options=sasl_options,
        )

        self._sasl_options = sasl_options
        self._pseudo_root = pseudo_root

        if autostart:
            self.start()

    def start(self):
        if self._started:
            raise KazooLocustStarted()
        self._client.start()
        self._started = True
        # Messy.
        self.ensure_pseudo_root()

    def stop(self):
        self.get_kazoo_client().stop()
        self._started = False

    def has_sasl_auth(self):
        return self._sasl_options is not None

    def get_kazoo_client(self):
        if self._started:
            return self._client
        raise KazooLocustStopped()

    def ensure_pseudo_root(self):
        if self._pseudo_root:
            try:
                self.get_kazoo_client().create(self._pseudo_root)
            except NodeExistsError:
                pass

    def join_path(self, path):
        if self._pseudo_root:
            return self._pseudo_root + path
        else:
            return path

    def create_default_node(self):
        path = self.join_path('/d-')
        client = self.get_kazoo_client()

        return client.create(path,
                             ephemeral=True,
                             sequence=True)


class KazooLocust(Locust):
    def __init__(self, *args, **kwargs):
        super(KazooLocust, self).__init__()

        self.client = KazooLocustClient(*args, **kwargs)

    # TODO(ddiederen): Teardown is a class method!  Cannot do this
    # here.
    # def teardown(self):
    #     self.client.stop()


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
            if isinstance(value, KazooException):
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
