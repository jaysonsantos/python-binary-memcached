import unittest

import bmemcached


THREE_SERVERS = ['localhost:11211', 'localhost:11212', 'localhost:11213']


class DistributedClientHashingTest(unittest.TestCase):
    def test_get_server_is_consistent(self):
        key = 'the_key'

        for _ in range(10):
            client = bmemcached.DistributedClient(THREE_SERVERS)
            self.assertEqual(client._get_server(key).port, 11211)

    def test_same_server_list_gives_the_same_ring(self):
        """
        Two clients built from the same server list agree on every key.
        """
        first = bmemcached.DistributedClient(THREE_SERVERS)
        second = bmemcached.DistributedClient(THREE_SERVERS)

        for index in range(200):
            key = 'key_{}'.format(index)
            self.assertEqual(first._get_server(key).port, second._get_server(key).port)

    def test_removing_a_server_moves_only_its_own_keys(self):
        """
        This is the property that consistent hashing exists for.

        A plain modulo hash remaps almost every key when the server count
        changes. A consistent hash ring must move only the keys that lived on
        the server that left.
        """
        removed_port = 11213
        remaining = [server for server in THREE_SERVERS if not server.endswith(str(removed_port))]

        before = bmemcached.DistributedClient(THREE_SERVERS)
        after = bmemcached.DistributedClient(remaining)

        keys = ['key_{}'.format(index) for index in range(500)]
        moved = []
        for key in keys:
            old_port = before._get_server(key).port
            new_port = after._get_server(key).port
            if old_port != new_port:
                moved.append((key, old_port, new_port))

        # No key that lived on a surviving server is allowed to move.
        for key, old_port, new_port in moved:
            self.assertEqual(
                removed_port, old_port,
                'key {} moved from {} to {}, but {} was still in the ring'.format(
                    key, old_port, new_port, old_port))

        # Every key that lived on the removed server must land somewhere else.
        for key in keys:
            if before._get_server(key).port == removed_port:
                self.assertIn(after._get_server(key).port, (11211, 11212))

    def test_adding_a_server_takes_keys_only_from_the_others(self):
        """
        An added server takes a share of the keys. No key moves between two
        servers that were both already in the ring.
        """
        added_port = 11214
        larger = THREE_SERVERS + ['localhost:{}'.format(added_port)]

        before = bmemcached.DistributedClient(THREE_SERVERS)
        after = bmemcached.DistributedClient(larger)

        keys = ['key_{}'.format(index) for index in range(500)]
        moved_to_new_server = 0
        for key in keys:
            old_port = before._get_server(key).port
            new_port = after._get_server(key).port
            if old_port == new_port:
                continue
            self.assertEqual(
                added_port, new_port,
                'key {} moved from {} to {}, but neither server is new'.format(
                    key, old_port, new_port))
            moved_to_new_server += 1

        # The new server must actually receive keys. A ring that gave it none
        # would pass the check above without testing anything.
        self.assertGreater(moved_to_new_server, 0)

    def test_every_server_receives_keys(self):
        client = bmemcached.DistributedClient(THREE_SERVERS)
        ports = {client._get_server('key_{}'.format(index)).port for index in range(500)}
        self.assertEqual({11211, 11212, 11213}, ports)
