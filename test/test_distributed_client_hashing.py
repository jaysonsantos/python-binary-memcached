import unittest
from unittest import mock

import bmemcached


class DistributedClientHashingTest(unittest.TestCase):
    def test_get_server_is_consistent(self):
        key = "the_key"
        servers = ["localhost:11211", "localhost:11212", "localhost:11213"]

        for _ in range(10):
            client = bmemcached.DistributedClient(servers)
            self.assertEqual(client._get_server(key).port, 11211)

    def test_delete_multi_calls_every_server_after_a_failure(self):
        client = bmemcached.DistributedClient(["localhost:11211", "localhost:11212"])
        keys = [f"key{i}" for i in range(20)]
        self.assertEqual(len({client._get_server(key) for key in keys}), 2)

        with mock.patch.object(
            bmemcached.protocol.Protocol, "delete_multi", autospec=True, return_value=False
        ) as delete_multi:
            self.assertFalse(client.delete_multi(keys))

        self.assertEqual({call.args[0] for call in delete_multi.call_args_list}, set(client._servers))
