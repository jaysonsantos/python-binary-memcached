import os
import unittest

import six

import bmemcached

if six.PY3:
    from unittest import mock
else:
    import mock


class TestServerParsing(unittest.TestCase):
    def testAcceptStringServer(self):
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']))
        self.assertEqual(len(list(client.servers)), 1)

    def testAcceptIterableServer(self):
        client = bmemcached.Client(
            ['{}:11211'.format(os.environ['MEMCACHED_HOST']), '{}:11211'.format(os.environ['MEMCACHED_HOST'])])
        self.assertEqual(len(list(client.servers)), 2)

    def testNoPortGiven(self):
        server = bmemcached.protocol.Protocol(os.environ['MEMCACHED_HOST'])
        self.assertEqual(server.host, os.environ['MEMCACHED_HOST'])
        self.assertEqual(server.port, 11211)

    def testIPv6(self):
        server = bmemcached.protocol.Protocol('[::1]')
        self.assertEqual(server.host, '::1')
        self.assertEqual(server.port, 11211)
        server = bmemcached.protocol.Protocol('::1')
        self.assertEqual(server.host, '::1')
        self.assertEqual(server.port, 11211)
        server = bmemcached.protocol.Protocol('[2001:db8::2]')
        self.assertEqual(server.host, '2001:db8::2')
        self.assertEqual(server.port, 11211)
        server = bmemcached.protocol.Protocol('2001:db8::2')
        self.assertEqual(server.host, '2001:db8::2')
        self.assertEqual(server.port, 11211)
        # Since `2001:db8::2:8080` is a valid IPv6 address,
        # it is ambiguous whether to split it into `2001:db8::2` and `8080`
        # or treat it as `2001:db8::2:8080`.
        # Therefore, it will be treated as `2001:db8::2:8080`.
        server = bmemcached.protocol.Protocol('2001:db8::2:8080')
        self.assertEqual(server.host, '2001:db8::2:8080')
        self.assertEqual(server.port, 11211)
        server = bmemcached.protocol.Protocol('[::1]:5000')
        self.assertEqual(server.host, '::1')
        self.assertEqual(server.port, 5000)
        server = bmemcached.protocol.Protocol('[2001:db8::2]:5000')
        self.assertEqual(server.host, '2001:db8::2')
        self.assertEqual(server.port, 5000)

    def testInvalidPort(self):
        with self.assertRaises(ValueError):
            bmemcached.protocol.Protocol('{}:blah'.format(os.environ['MEMCACHED_HOST']))
        with self.assertRaises(ValueError):
            bmemcached.protocol.Protocol('[::1]:blah')

    def testNonStandardPort(self):
        server = bmemcached.protocol.Protocol('{}:5000'.format(os.environ['MEMCACHED_HOST']))
        self.assertEqual(server.host, os.environ['MEMCACHED_HOST'])
        self.assertEqual(server.port, 5000)

    def testAcceptUnixSocket(self):
        client = bmemcached.Client('/tmp/memcached.sock')
        self.assertEqual(len(list(client.servers)), 1)

    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testPassCredentials(self, mocked_response):
        """
        If username/password passed to Client, auto-authenticate.
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0, 0, 0, 0, [b'PLAIN'])
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), username='user',
                                   password='password')
        server = list(client.servers)[0]

        # Force a connection.  Normally this is only done when we make a request to the
        # server.
        server._send_authentication()

        self.assertTrue(server.authenticated)

    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testNoCredentialsNoAuth(self, mocked_response):
        mocked_response.return_value = (0, 0, 0, 0, 0, 0x01, 0, 0, 0, [b'PLAIN'])
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']))
        server = list(client.servers)[0]

        # Force a connection.  Normally this is only done when we make a request to the
        # server.
        server._send_authentication()

        self.assertFalse(server.authenticated)

    def testNoServersSupplied(self):
        """
        Raise assertion if the server list is empty.
        """
        self.assertRaises(AssertionError, bmemcached.Client, [])
