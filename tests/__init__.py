import unittest
import bmemcached
import os


class MainTests(unittest.TestCase):
    def setUp(self):
        self.client = bmemcached.Client(('127.0.0.1:11211', ), 'user',
            'password')

    def testSet(self):
        self.assertEqual(0, self.client.set('test', 'test'))

    def testGet(self):
        self.client.set('test', 'test')
        self.assertEqual('test', self.client.get('test'))


class TestAuthentication(unittest.TestCase):
    def setUp(self):
        self.server = bmemcached.Server(os.environ.get('MEMCACHED_SERVER',
            '127.0.0.1'))

    def testAuth(self):
        self.assertTrue(self.server.authenticate(
            os.environ.get('MEMCACHED_USERNAME', 'user'),
            os.environ.get('MEMCACHED_PASSWORD', 'password')))
