import unittest
import bmemcached


class DistributedClientHashingTest(unittest.TestCase):
    def test_get_server_is_consistent(self):
        key = 'the_key'
        servers = ['localhost:11211', 'localhost:11212', 'localhost:11213']

        for _ in range(10):
            client = bmemcached.DistributedClient(servers)
            self.assertEqual(client._get_server(key).port, 11211)
