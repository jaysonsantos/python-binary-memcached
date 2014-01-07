import mock
import unittest
import bmemcached


class TestServerParsing(unittest.TestCase):
    def testAcceptStringServer(self):
        client = bmemcached.Client('127.0.0.1:11211')
        self.assertEqual(len(list(client.servers)), 1)

    def testAcceptIterableServer(self):
        client = bmemcached.Client(['127.0.0.1:11211', '127.0.0.1:11211'])
        self.assertEqual(len(list(client.servers)), 2)

    def testNoPortGiven(self):
        server = bmemcached.client.Protocol('127.0.0.1')
        self.assertEqual(server.host, '127.0.0.1')
        self.assertEqual(server.port, 11211)

    def testInvalidPort(self):
        server = bmemcached.client.Protocol('127.0.0.1:blah')
        self.assertEqual(server.host, '127.0.0.1')
        self.assertEqual(server.port, 11211)

    def testNonStandardPort(self):
        server = bmemcached.client.Protocol('127.0.0.1:5000')
        self.assertEqual(server.host, '127.0.0.1')
        self.assertEqual(server.port, 5000)

    def testAcceptUnixSocket(self):
        client = bmemcached.Client('/tmp/memcached.sock')
        self.assertEqual(len(list(client.servers)), 1)

    @mock.patch.object(bmemcached.client.Protocol, '_get_response')
    def testPassCredentials(self, mocked_response):
        """
        If username/password passed to Client, auto-authenticate.
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0, 0, 0, 0, ['PLAIN'])
        client = bmemcached.Client('127.0.0.1:11211', username='user',
                                   password='password')
        server = list(client.servers)[0]
        self.assertTrue(server.authenticated)

    @mock.patch.object(bmemcached.client.Protocol, '_get_response')
    def testNoCredentialsNoAuth(self, mocked_response):
        mocked_response.return_value = (0, 0, 0, 0, 0, 0x01, 0, 0, 0, ['PLAIN'])
        client = bmemcached.Client('127.0.0.1:11211')
        server = list(client.servers)[0]
        self.assertFalse(server.authenticated)

    def testNoServersSupplied(self):
        """
        Raise assertion if the server list is empty.
        """
        self.assertRaises(AssertionError, bmemcached.Client, [])
