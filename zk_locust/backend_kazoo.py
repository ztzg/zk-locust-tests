import os
import importlib
import logging
import json

import kazoo.handlers.gevent
import kazoo.handlers.threading
import kazoo.client
import kazoo.exceptions
from kazoo.protocol.states import KeeperState

from .backend_base import ZKLocustException, AbstractZKLocustClient

_logger = logging.getLogger(__name__)


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


_global_handler = os.getenv('KAZOO_LOCUST_HANDLER')
_global_timeout_s = os.getenv('KAZOO_LOCUST_TIMEOUT_S')


def fetch_global_sasl_options():
    s = os.getenv('KAZOO_LOCUST_SASL_OPTIONS')
    if not s:
        return None
    return json.loads(s)


_global_sasl_options = fetch_global_sasl_options()

_create_kazoo_client_var = 'KAZOO_LOCUST_CREATE_CLIENT'
_create_kazoo_client_spec = os.getenv(_create_kazoo_client_var)

if _create_kazoo_client_spec:

    fn = None

    try:
        module_name, fn_name = _create_kazoo_client_spec.rsplit(
            '.', maxsplit=1)
        module = importlib.import_module(module_name)
        fn = getattr(module, fn_name)
    except Exception:
        _logger.exception()

    if not callable(fn):
        raise KazooLocustException(
            'Failed to resolve ' + _create_kazoo_client_var + ' spec ' +
            repr(_create_kazoo_client_spec) + ' to a callable')

    _create_kazoo_client_fn = fn
else:
    _create_kazoo_client_fn = kazoo.client.KazooClient


class KazooLocustClient(AbstractZKLocustClient):
    _started = False
    _sasl_options = None
    _pseudo_root = None

    def __init__(self,
                 hosts,
                 pseudo_root,
                 handler=_global_handler,
                 sasl_options=_global_sasl_options,
                 timeout=None,
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

        if timeout is None and _global_timeout_s:
            timeout = float(_global_timeout_s)
        if timeout is not None:
            kwargs['timeout'] = timeout

        self._set_zk_client(_create_kazoo_client_fn(hosts=hosts, **kwargs))

        self._sasl_options = sasl_options

        if autostart:
            self.start()

    def node_exists_except(self):
        return kazoo.exceptions.NodeExistsError

    def start(self):
        if self._started:
            raise KazooLocustStartedException()
        super(KazooLocustClient, self).get_zk_client().start()
        self._started = True
        # Messy.
        self.ensure_pseudo_root()

    def stop(self):
        if self._started:
            super(KazooLocustClient, self).get_zk_client().stop()
        self._started = False

    def has_sasl_auth(self):
        return self._sasl_options is not None

    def get_zk_client(self):
        if self._started:
            return super(KazooLocustClient, self).get_zk_client()
        raise KazooLocustStoppedException()

    def is_connection_down(self):
        if not self._started:
            return False
        return self.get_zk_client().client_state is KeeperState.CONNECTING

    def create_default_node(self):
        path = self.join_path('/d-')
        client = self.get_zk_client()

        return client.create(path, ephemeral=True, sequence=True)


KAZOO_EXCEPTIONS = (kazoo.exceptions.KazooException, )
KAZOO_NON_SUPPRESS_EXCEPTIONS = (kazoo.exceptions.SessionExpiredError, )
