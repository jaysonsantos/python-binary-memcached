import os
import unittest

import six

import bmemcached
from bmemcached.exceptions import AuthenticationNotSupported, InvalidCredentials, MemcachedException

if six.PY3:
    from unittest import mock
else:
    import mock


class TestServerAuth(unittest.TestCase):
    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testServerDoesntNeedAuth(self, mocked_response):
        """
        If 0x81 ('unkown_command') comes back in the status field when
        authenticating, it isn't needed.
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
        server = bmemcached.protocol.Protocol(os.environ['MEMCACHED_HOST'])
        # can pass anything and it'll work
        self.assertTrue(server.authenticate('user', 'badpassword'))

    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testNotUsingPlainAuth(self, mocked_response):
        """
        Raise AuthenticationNotSupported unless we're using PLAIN auth.
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0, 0, 0, 0, [])
        server = bmemcached.protocol.Protocol(os.environ['MEMCACHED_HOST'])
        self.assertRaises(AuthenticationNotSupported,
                          server.authenticate, 'user', 'password')

    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testAuthNotSuccessful(self, mocked_response):
        """
        Raise MemcachedException for anything unsuccessful.
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0x01, 0, 0, 0, [b'PLAIN'])
        server = bmemcached.protocol.Protocol(os.environ['MEMCACHED_HOST'])
        self.assertRaises(MemcachedException,
                          server.authenticate, 'user', 'password')

    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testAuthSuccessful(self, mocked_response):
        """
        Valid logins return True.
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0, 0, 0, 0, [b'PLAIN'])
        server = bmemcached.protocol.Protocol(os.environ['MEMCACHED_HOST'])
        self.assertTrue(server.authenticate('user', 'password'))

    @mock.patch.object(bmemcached.protocol.Protocol, '_get_response')
    def testAuthUnsuccessful(self, mocked_response):
        """
        Invalid logins raise InvalidCredentials
        """
        mocked_response.return_value = (0, 0, 0, 0, 0, 0x08, 0, 0, 0, [b'PLAIN'])
        server = bmemcached.protocol.Protocol(os.environ['MEMCACHED_HOST'])
        self.assertRaises(InvalidCredentials, server.authenticate,
                          'user', 'password2')
