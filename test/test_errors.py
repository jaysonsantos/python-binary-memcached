import os
import socket
import struct
import unittest

import six

import bmemcached
from bmemcached.exceptions import AuthenticationNotSupported, InvalidCredentials, MemcachedException

if six.PY3:
    from unittest import mock
else:
    import mock


class TestMemcachedErrors(unittest.TestCase):
    def testGet(self):
        """
        Raise MemcachedException if request wasn't successful and
        wasn't a 'key not found' error.
        """
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), 'user', 'password')
        with mock.patch.object(bmemcached.protocol.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.get, 'foo')

    def testSet(self):
        """
        Raise MemcachedException if request wasn't successful and
        wasn't a 'key not found' or 'key exists' error.
        """
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), 'user', 'password')
        with mock.patch.object(bmemcached.protocol.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.set, 'foo', 'bar', 300)

    def testIncrDecr(self):
        """
        Incr/Decr raise MemcachedException unless the request wasn't
        successful.
        """
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), 'user', 'password')
        client.set('foo', 1)
        with mock.patch.object(bmemcached.protocol.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 2)
            self.assertRaises(MemcachedException, client.incr, 'foo', 1)
            self.assertRaises(MemcachedException, client.decr, 'foo', 1)

    def testDelete(self):
        """
        Raise MemcachedException if the delete request isn't successful.
        """
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), 'user', 'password')
        client.flush_all()
        with mock.patch.object(bmemcached.protocol.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.delete, 'foo')

    def testFlushAll(self):
        """
        Raise MemcachedException if the flush wasn't successful.
        """
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), 'user', 'password')
        with mock.patch.object(bmemcached.protocol.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.flush_all)


class TestMemcachedExceptionMessage(unittest.TestCase):
    def testStrHoldsTheMessage(self):
        """
        str() on a MemcachedException returns the message, not a tuple repr.
        """
        exc = MemcachedException('boom', 1)
        self.assertIn('boom', str(exc))
        self.assertEqual('boom', exc.message)
        self.assertEqual(1, exc.code)

    def testSubclassesStrHoldsTheMessage(self):
        """
        The subclasses inherit the usable message.
        """
        for cls in (AuthenticationNotSupported, InvalidCredentials):
            self.assertIn('boom', str(cls('boom', 1)))


class TestBadResponseHeader(unittest.TestCase):
    def setUp(self):
        self.server = bmemcached.protocol.Protocol('{}:11211'.format(os.environ['MEMCACHED_HOST']))

    def tearDown(self):
        self.server.disconnect()

    def _read_socket_returning(self, header):
        def _read_socket(size):
            return header[:size]
        return _read_socket

    def testMalformedHeaderRaisesAndDisconnects(self):
        """
        A response header with a bad magic byte raises a clear exception.

        The socket is dropped, because the body was never read. A reused socket
        would read the leftover body as the next header.
        """
        # A well-formed 24 byte header with the request magic instead of the
        # response magic. bodylen is non-zero, so the body is still unread.
        bad_header = struct.pack(bmemcached.protocol.Protocol.HEADER_STRUCT,
                                 0x80, 0x00, 0, 0, 0, 0, 16, 0, 0)

        with mock.patch.object(bmemcached.protocol.Protocol, '_read_socket',
                               side_effect=self._read_socket_returning(bad_header)):
            with mock.patch.object(bmemcached.protocol.Protocol, '_open_connection'):
                self.server.connection = mock.Mock()
                with self.assertRaises(MemcachedException) as caught:
                    self.server._get_response()

        self.assertIn('magic', str(caught.exception))
        self.assertIsNone(self.server.connection)

    def testShortHeaderRaisesSocketError(self):
        """
        A short read still surfaces as a server_disconnected status.
        """
        with mock.patch.object(bmemcached.protocol.Protocol, '_read_socket',
                               side_effect=socket.error('short read')):
            with mock.patch.object(bmemcached.protocol.Protocol, '_open_connection'):
                self.server.connection = mock.Mock()
                response = self.server._get_response()

        self.assertEqual(bmemcached.protocol.Protocol.STATUS['server_disconnected'], response[5])


class TestInvalidCasValue(unittest.TestCase):
    def testCasZeroRaisesValueError(self):
        """
        A CAS value of 0 means "no cas" on the wire. A cas() call with 0 would
        become an unconditional overwrite, so reject it.
        """
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']), 'user', 'password')
        self.assertRaises(ValueError, client.cas, 'foo', 'bar', 0)
        client.disconnect_all()
