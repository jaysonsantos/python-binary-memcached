import os
import pickle
import unittest
import zlib
from unittest import mock

import bmemcached
from bmemcached.exceptions import MemcachedException
from bmemcached.protocol import FLAGS_UNPACKER, Protocol

KEY = "raw_data_key"
OTHER_KEY = "raw_data_key2"

# These bytes are not valid zlib data, not a valid pickle, and not a number.
# A default client fails to read them when a type flag is set.
FOREIGN_VALUE = b"\x00\x01owned by another application\xff"


def set_with_flags(server, key, value, flags):
    """
    Store a value with the given flags, as another application does.
    """
    protocol = Protocol(server)
    keybytes = key.encode()
    klen = len(keybytes)
    cmd = Protocol.COMMANDS["set"]
    protocol._send(
        cmd["packer"].pack(
            Protocol.MAGIC["request"], cmd["command"], klen, 8, 0, 0, klen + len(value) + 8, 0, 0, flags, 0
        )
        + keybytes
        + value
    )
    status = protocol._get_response()[5]
    protocol.disconnect()
    assert status == Protocol.STATUS["success"]


def get_with_flags(server, key):
    """
    Return the (flags, value) pair that the server holds for a key.
    """
    protocol = Protocol(server)
    keybytes = key.encode()
    klen = len(keybytes)
    cmd = Protocol.COMMANDS["get"]
    protocol._send(cmd["packer"].pack(Protocol.MAGIC["request"], cmd["command"], klen, 0, 0, 0, klen, 0, 0) + keybytes)
    response = protocol._get_response()
    protocol.disconnect()
    assert response[5] == Protocol.STATUS["success"]
    body = response[-1]
    (flags,) = FLAGS_UNPACKER.unpack_from(body)
    return flags, body[4:]


class RawDataTests(unittest.TestCase):
    def setUp(self):
        self.server = "{}:11211".format(os.environ["MEMCACHED_HOST"])
        self.client = bmemcached.Client(self.server)
        self.raw_client = bmemcached.Client(self.server, raw_data=True)
        self.reset()

    def tearDown(self):
        self.reset()
        self.client.disconnect_all()
        self.raw_client.disconnect_all()

    def reset(self):
        self.client.delete_multi([KEY, OTHER_KEY])

    def test_raw_data_defaults_to_false(self):
        self.assertFalse(Protocol(self.server).raw_data)
        self.assertFalse(self.client.raw_data)
        self.assertFalse(any(server.raw_data for server in self.client.servers))
        self.assertFalse(bmemcached.DistributedClient(self.server).raw_data)

    def test_round_trip(self):
        value = b"\x01z\x7f\x00\x80\xfe\xff\x00"
        self.assertTrue(self.raw_client.set(KEY, value))
        self.assertEqual(value, self.raw_client.get(KEY))

    def test_write_sends_flags_zero(self):
        self.raw_client.set(KEY, b"value")
        self.assertEqual((0, b"value"), get_with_flags(self.server, KEY))

    def test_read_ignores_flags_of_another_application(self):
        cases = [
            (Protocol.FLAGS["compressed"], zlib.error),
            (Protocol.FLAGS["object"], pickle.UnpicklingError),
            (Protocol.FLAGS["integer"], ValueError),
            (Protocol.FLAGS["long"], ValueError),
            (0xFFFFFFFF, zlib.error),
        ]
        for flags, error in cases:
            with self.subTest(flags=flags):
                set_with_flags(self.server, KEY, FOREIGN_VALUE, flags)

                # A default client reads the flags as its own type flags.
                self.assertRaises(error, self.client.get, KEY)
                self.assertEqual(FOREIGN_VALUE, self.raw_client.get(KEY))
                self.assertEqual({KEY: FOREIGN_VALUE}, self.raw_client.get_multi([KEY]))

    def test_read_returns_the_bytes_that_a_default_client_stored(self):
        value = {"number": 1}
        self.client.set(KEY, value)

        stored = self.raw_client.get(KEY)
        self.assertIsInstance(stored, bytes)
        self.assertEqual(value, pickle.loads(stored))

    def test_write_overwrites_flags_of_another_application(self):
        set_with_flags(self.server, KEY, FOREIGN_VALUE, Protocol.FLAGS["compressed"])
        self.raw_client.set(KEY, b"new value")
        self.assertEqual((0, b"new value"), get_with_flags(self.server, KEY))

    def test_set_rejects_values_that_are_not_bytes(self):
        for value in ("text", 42, 1.5, None, bytearray(b"value"), {"number": 1}):
            with self.subTest(value=value):
                self.assertRaises(TypeError, self.raw_client.set, KEY, value)
                self.assertIsNone(self.raw_client.get(KEY))

    def test_set_rejects_compression(self):
        for compress_level in (1, 9):
            with self.subTest(compress_level=compress_level):
                self.assertRaises(MemcachedException, self.raw_client.set, KEY, b"value", compress_level=compress_level)
                self.assertIsNone(self.raw_client.get(KEY))

    def test_set_accepts_compress_level_zero(self):
        self.assertTrue(self.raw_client.set(KEY, b"value", compress_level=0))
        self.assertEqual(b"value", self.raw_client.get(KEY))

    def test_large_value_is_not_compressed(self):
        value = b"a" * (Protocol.COMPRESSION_THRESHOLD * 8)
        compression = mock.Mock()
        for server in self.raw_client.servers:
            server.compression = compression

        self.assertTrue(self.raw_client.set(KEY, value))

        self.assertEqual((0, value), get_with_flags(self.server, KEY))
        self.assertEqual(value, self.raw_client.get(KEY))
        compression.compress.assert_not_called()
        compression.decompress.assert_not_called()

    def test_set_multi_and_get_multi(self):
        mappings = {KEY: b"one", OTHER_KEY: b"\x00\xff" * Protocol.COMPRESSION_THRESHOLD}
        self.assertEqual([], self.raw_client.set_multi(mappings))
        self.assertEqual(mappings, self.raw_client.get_multi([KEY, OTHER_KEY]))
        self.assertEqual((0, mappings[OTHER_KEY]), get_with_flags(self.server, OTHER_KEY))

    def test_set_multi_rejects_values_that_are_not_bytes(self):
        self.assertRaises(TypeError, self.raw_client.set_multi, {KEY: b"one", OTHER_KEY: "two"})
        self.assertRaises(MemcachedException, self.raw_client.set_multi, {KEY: b"one"}, compress_level=9)
        # The client found the error before it sent a request.
        self.assertEqual({}, self.raw_client.get_multi([KEY, OTHER_KEY]))

    def test_add_replace_and_cas(self):
        self.assertTrue(self.raw_client.add(KEY, b"added"))
        self.assertFalse(self.raw_client.add(KEY, b"added again"))
        self.assertTrue(self.raw_client.replace(KEY, b"replaced"))

        value, cas = self.raw_client.gets(KEY)
        self.assertEqual(b"replaced", value)
        self.assertTrue(self.raw_client.cas(KEY, b"swapped", cas))
        self.assertEqual((0, b"swapped"), get_with_flags(self.server, KEY))

        result = self.raw_client.set_multi_cas({KEY: b"one", OTHER_KEY: b"two"})
        self.assertIsNotNone(result[KEY])
        self.assertIsNotNone(result[OTHER_KEY])
        self.assertEqual({KEY: b"one", OTHER_KEY: b"two"}, self.raw_client.get_multi([KEY, OTHER_KEY]))
        self.assertRaises(TypeError, self.raw_client.set_multi_cas, {KEY: "one"})


class DistributedRawDataTests(unittest.TestCase):
    def setUp(self):
        host = os.environ["MEMCACHED_HOST"]
        self.servers = [f"{host}:11211", f"{host}:5000"]
        self.raw_client = bmemcached.DistributedClient(self.servers, raw_data=True)
        self.mappings = {f"raw_data_distributed_{index}": f"value {index}".encode() for index in range(20)}
        self.raw_client.delete_multi(self.mappings)

    def tearDown(self):
        self.raw_client.delete_multi(self.mappings)
        self.raw_client.disconnect_all()

    def test_raw_data_reaches_every_server(self):
        self.assertTrue(self.raw_client.raw_data)
        self.assertEqual([True, True], [server.raw_data for server in self.raw_client.servers])

    def test_raw_data_is_the_last_positional_argument(self):
        client = bmemcached.DistributedClient(
            self.servers, None, None, None, 3, 0, pickle.Pickler, pickle.Unpickler, None, True
        )
        self.assertEqual([True, True], [server.raw_data for server in client.servers])

    def test_round_trip(self):
        # The keys must spread over the two servers, or one server stays untested.
        self.assertEqual(2, len({self.raw_client._get_server(key) for key in self.mappings}))

        for key, value in self.mappings.items():
            self.assertTrue(self.raw_client.set(key, value))
        for key, value in self.mappings.items():
            self.assertEqual(value, self.raw_client.get(key))

    def test_set_multi_and_get_multi(self):
        self.assertEqual([], self.raw_client.set_multi(self.mappings))
        self.assertEqual(self.mappings, self.raw_client.get_multi(list(self.mappings)))

    def test_set_rejects_values_that_are_not_bytes(self):
        key = next(iter(self.mappings))
        self.assertRaises(TypeError, self.raw_client.set, key, "text")
        self.assertRaises(MemcachedException, self.raw_client.set, key, b"value", compress_level=9)
        self.assertIsNone(self.raw_client.get(key))
