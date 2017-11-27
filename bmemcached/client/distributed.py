from uhashring import HashRing

from bmemcached.client import SOCKET_TIMEOUT
from bmemcached.client.mixin import ClientMixin
from bmemcached.compat import pickle


class DistributedClient(ClientMixin):
    """This is intended to be a client class which implement standard cache interface that common libs do...

    It tries to distribute keys over the specified servers using `HashRing` consistent hash.
    """
    def __init__(self, servers=('127.0.0.1:11211',), username=None, password=None, compression=None,
                 socket_timeout=SOCKET_TIMEOUT, pickle_protocol=0, pickler=pickle.Pickler, unpickler=pickle.Unpickler):
        super(DistributedClient, self).__init__(servers, username, password, compression, socket_timeout,
                                                pickle_protocol, pickler, unpickler)
        self._ring = HashRing(self._servers)

    def _get_server(self, key):
        return self._ring.get_node(key)

    def delete(self, key, cas=0):
        server = self._get_server(key)
        return server.delete(key, cas)

    def set(self, key, value, time=0, compress_level=-1):
        server = self._get_server(key)
        return server.set(key, value, time, compress_level)

    def add(self, key, value, time=0, compress_level=-1):
        server = self._get_server(key)
        return server.add(key, value, time, compress_level)

    def replace(self, key, value, time=0, compress_level=-1):
        server = self._get_server(key)
        return server.replace(key, value, time, compress_level)

    def get(self, key, get_cas=False):
        server = self._get_server(key)
        value, cas = server.get(key)
        # TODO: Check what is the right behavior when get_cas is True and key does not exist
        if value is not None:
            if get_cas:
                return value, cas
            return value

        if get_cas:
            return None, None

    def gets(self, key):
        server = self._get_server(key)
        return server.get(key)

    def cas(self, key, value, cas, time=0, compress_level=-1):
        server = self._get_server(key)
        return server.cas(key, value, cas, time, compress_level)

    def incr(self, key, value):
        server = self._get_server(key)
        return server.incr(key, value)

    def decr(self, key, value):
        server = self._get_server(key)
        return server.decr(key, value)
