import bmemcached
from tests.test_simple_functions import MemcachedTests


class SocketMemcachedTests(MemcachedTests):
    """
    Same tests as above, just make sure it works with sockets.
    """
    def setUp(self):
        self.server = '/tmp/memcached.sock'
        self.client = bmemcached.Client(self.server, 'user', 'password')
