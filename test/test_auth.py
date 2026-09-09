import os
import unittest


import bmemcached
from bmemcached.exceptions import AuthenticationNotSupported, InvalidCredentials, MemcachedException

from unittest import mock


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


class TestProtocolStrDoesNotLeakPassword(unittest.TestCase):
    def testStrHoldsNoPassword(self):
        """
        str(Protocol) reaches log lines, the repr of a server list, and any
        traceback that prints the object. The password must not be in it.
        """
        password = 'a-very-secret-password'
        server = bmemcached.protocol.Protocol('{}:11211'.format(os.environ['MEMCACHED_HOST']),
                                              username='user', password=password)
        rendered = str(server)
        self.assertNotIn(password, rendered)
        self.assertIn('user', rendered)

    def testReprOfServerListHoldsNoPassword(self):
        password = 'another-secret'
        client = bmemcached.Client('{}:11211'.format(os.environ['MEMCACHED_HOST']),
                                   'user', password)
        self.assertNotIn(password, str([str(s) for s in client.servers]))
        client.disconnect_all()
