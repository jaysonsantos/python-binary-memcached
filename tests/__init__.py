from mock import Mock, patch
import unittest
import bmemcached
import os


class MainTests(unittest.TestCase):
    def setUp(self):
        self.client = bmemcached.Client(('127.0.0.1:11211', ), 'user',
            'password')

    def tearDown(self):
        self.client.delete('test_key')
        self.client.delete('test_key2')
        self.client.disconnect_all()

    def testSet(self):
        self.assertTrue(self.client.set('test_key', 'test'))

    def testSetMulti(self):
        self.assertTrue(self.client.set_multi({'test_key': 'value',
            'test_key2': 'value2'}))

    def testGet(self):
        self.client.set('test_key', 'test')
        self.assertEqual('test', self.client.get('test_key'))

    def testGetMulti(self):
        self.assertTrue(self.client.set_multi({'test_key': 'value',
            'test_key2': 'value2'}))
        self.assertEqual({'test_key': 'value', 'test_key2': 'value2'},
            self.client.get_multi(['test_key', 'test_key2']))

    def testGetLong(self):
        self.client.set('test_key', 1L)
        value = self.client.get('test_key')
        self.assertEqual(1L, value)
        self.assertTrue(isinstance(value, long))

    def testGetInteger(self):
        self.client.set('test_key', 1)
        value = self.client.get('test_key')
        self.assertEqual(1, value)
        self.assertTrue(isinstance(value, int))

    def testGetObject(self):
        self.client.set('test_key', {'a': 1})
        value = self.client.get('test_key')
        self.assertTrue(isinstance(value, dict))
        self.assertTrue('a' in value)
        self.assertEqual(1, value['a'])

    def testDelete(self):
        self.client.set('test_key', 'test')
        self.assertTrue(self.client.delete('test_key'))
        self.assertEqual(None, self.client.get('test_key'))

    def testDeleteUnknownKey(self):
        self.assertTrue(self.client.delete('test_key'))

    def testAddPass(self):
        self.assertTrue(self.client.add('test_key', 'test'))

    def testAddFail(self):
        self.client.add('test_key', 'value')
        with self.assertRaises(ValueError) as exp:
            self.client.add('test_key', 'value')

    def testReplacePass(self):
        self.client.add('test_key', 'value')
        self.assertTrue(self.client.replace('test_key', 'value2'))
        self.assertEqual('value2', self.client.get('test_key'))

    def testReplaceFail(self):
        with self.assertRaises(ValueError) as exp:
            self.client.replace('test_key', 'value2')

    def testIncrement(self):
        self.assertEqual(0, self.client.incr('test_key', 1))
        self.assertEqual(1, self.client.incr('test_key', 1))

    def testDecrement(self):
        self.assertEqual(0, self.client.decr('test_key', 1))
        self.assertEqual(0, self.client.decr('test_key', 1))

    def testFlush(self):
        self.client.set('test_key', 'test')
        self.assertTrue(self.client.flush_all())
        self.assertEqual(None, self.client.get('test_key'))

    def testStats(self):
        stats = self.client.stats()['127.0.0.1:11211']
        self.assertTrue('pid' in stats)

        stats = self.client.stats('settings')['127.0.0.1:11211']
        self.assertTrue('verbosity' in stats)

        stats = self.client.stats('slabs')['127.0.0.1:11211']
        self.assertTrue('1:get_hits' in stats)


class TestAuthentication(unittest.TestCase):
    def setUp(self):
        self.server = bmemcached.Server(os.environ.get('MEMCACHED_SERVER',
            '127.0.0.1'))

    def testAuth(self):
        self.assertTrue(self.server.authenticate(
            os.environ.get('MEMCACHED_USERNAME', 'user'),
            os.environ.get('MEMCACHED_PASSWORD', 'password')))


class TestWrongPort(unittest.TestCase):
    def testWrongPortFail(self):
        bmemcached.Client(('127.0.0.1:bla', ), 'user',
            'password')


class TestServerParsing(unittest.TestCase):
    def testAcceptStringServer(self):
        client = bmemcached.Client('127.0.0.1:11211')
        self.assertEqual(len(client.servers), 1)

    def testAcceptIterableServer(self):
        client = bmemcached.Client(['127.0.0.1:11211', '127.0.0.1:11211'])
        self.assertEqual(len(client.servers), 2)

    def testNoPortGiven(self):
        server = bmemcached.Server('127.0.0.1')
        self.assertEqual(server.host, '127.0.0.1')
        self.assertEqual(server.port, 11211)

    def testInvalidPort(self):
        server = bmemcached.Server('127.0.0.1:blah')
        self.assertEqual(server.host, '127.0.0.1')
        self.assertEqual(server.port, 11211)

    def testNonStandardPort(self):
        server = bmemcached.Server('127.0.0.1:5000')
        self.assertEqual(server.host, '127.0.0.1')
        self.assertEqual(server.port, 5000)

    def testAcceptUnixSocket(self):
        client = bmemcached.Client('/tmp/memcached.sock')
        self.assertEqual(len(client.servers), 1)

    def testPassCredentials(self):
        """
        If username/password passed to Client, auto-authenticate.
        """
        client = bmemcached.Client('127.0.0.1:11211', username='user',
                                   password='password')
        server = client.servers[0]
        self.assertTrue(server.authenticated)

    def testNoCredentialsNoAuth(self):
        client = bmemcached.Client('127.0.0.1:11211')
        server = client.servers[0]
        self.assertFalse(server.authenticated)


class TestServerAuth(unittest.TestCase):
    @patch.object(bmemcached.Server, '_get_response')
    def testServerDoesntNeedAuth(self, mock):
        """
        If 0x81 ('unkown_command') comes back in the status field when
        authenticating, it isn't needed.
        """
        mock.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
        server = bmemcached.Server('127.0.0.1')
        # can pass anything and it'll work
        self.assertTrue(server.authenticate('user', 'badpassword'))

    @patch.object(bmemcached.Server, '_get_response')
    def testNotUsingPlainAuth(self, mock):
        """
        Raise AuthenticationNotSupported unless we're using PLAIN auth.
        """
        mock.return_value = (0, 0, 0, 0, 0, 0, 0, 0, 0, [])
        server = bmemcached.Server('127.0.0.1')
        self.assertRaises(bmemcached.AuthenticationNotSupported,
                          server.authenticate, 'user', 'password')

    @patch.object(bmemcached.Server, '_get_response')
    def testAuthNotSuccessful(self, mock):
        """
        Raise MemcachedException for anything unsuccessful.
        """
        mock.return_value = (0, 0, 0, 0, 0, 0x01, 0, 0, 0, ['PLAIN'])
        server = bmemcached.Server('127.0.0.1')
        self.assertRaises(bmemcached.MemcachedException,
                          server.authenticate, 'user', 'password')

