class ZKLocustException(Exception):
    pass


class AbstractZKLocustClient(object):
    _pseudo_root = None
    _zk_client = None

    def __init__(self, pseudo_root):
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
            except self.node_exists_except():
                pass

    def join_path(self, path):
        if self._pseudo_root:
            return self._pseudo_root + path
        else:
            return path
