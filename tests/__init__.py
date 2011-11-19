import unittest
import bmemcached


class MainTests(unittest.TestCase):
    def setUp(self):
        self.client = bmemcached.Client(('127.0.0.1:11211', ))
    
    def testSet(self):
        self.assertEqual(0, self.client.set('test', 'test'))
    
    def testGet(self):
        self.client.set('test', 'test')
        self.assertEqual('test', self.client.get('test'))