import test_simple_functions

import bmemcached


class SocketMemcachedTests(test_simple_functions.MemcachedTests):
    """
    Same tests as above, just make sure it works with sockets.
    """

    def setUp(self):
        self.server = '/tmp/memcached.sock'
        self.client = bmemcached.Client(self.server, 'user', 'password')
