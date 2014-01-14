import mock
import unittest
import bmemcached
from bmemcached.exceptions import AuthenticationNotSupported, InvalidCredentials, MemcachedException
from test_simple_functions import MemcachedTests


class TestServerAuth(MemcachedTests):
    @mock.patch('bmemcached.client.Protocol')
    def setUp(self, mocked_protocol):
        self.server1 = mock.Mock()
        self.server2 = mock.Mock()

        mocked_protocol.side_effect = iter([self.server1, self.server2])
        self.client = bmemcached.Client(['127.0.0.1:5000', '127.0.0.1:500'], consistent_hashing=True)

    @mock.patch('bmemcached.client.Protocol')
    def testConsistentHashing(self, mocked_protocol):

        client = bmemcached.Client(['127.0.0.1:5000', '127.0.0.1:500'], consistent_hashing=True)
        for i in range(3):
            assert self.server2 == client.get_server('testing-key')

        for i in range(3):
            assert self.server1 == client.get_server('testing-key2')

