import unittest
import bmemcached
import os


class MainTests(unittest.TestCase):
    def setUp(self):
        self.client = bmemcached.Client(('127.0.0.1:11211', ), 'user',
            'password')

    def tearDown(self):
        self.client.delete('test_key')
        self.client.disconnect_all()

    def testSet(self):
        self.assertEqual(True, self.client.set('test_key', 'test'))

    def testGet(self):
        self.client.set('test_key', 'test')
        self.assertEqual('test', self.client.get('test_key'))

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


class TestAuthentication(unittest.TestCase):
    def setUp(self):
        self.server = bmemcached.Server(os.environ.get('MEMCACHED_SERVER',
            '127.0.0.1'))

    def testAuth(self):
        self.assertTrue(self.server.authenticate(
            os.environ.get('MEMCACHED_USERNAME', 'user'),
            os.environ.get('MEMCACHED_PASSWORD', 'password')))
