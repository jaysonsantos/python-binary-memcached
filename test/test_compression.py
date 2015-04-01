import unittest
import bz2
import bmemcached


class MemcachedTests(unittest.TestCase):
    def setUp(self):
        self.server = '127.0.0.1:11211'
        self.client = bmemcached.Client(self.server, 'user', 'password')
        self.bzclient = bmemcached.Client(self.server, 'user', 'password',
                                          compression=bz2)
        self.data = b'this is test data. ' * 32

    def tearDown(self):
        self.client.delete('test_key')
        self.client.delete('test_key2')
        self.client.disconnect_all()
        self.bzclient.disconnect_all()

    def testCompressedData(self):
        self.client.set('test_key', self.data)
        self.assertEqual(self.data, self.client.get('test_key'))

    def testBZ2CompressedData(self):
        self.bzclient.set('test_key', self.data)
        self.assertEqual(self.data, self.bzclient.get('test_key'))

    def testCompressionMissmatch(self):
        self.client.set('test_key', self.data)
        self.bzclient.set('test_key2', self.data)
        self.assertEqual(self.client.get('test_key'),
                self.bzclient.get('test_key2'))
        self.assertRaises(IOError, self.bzclient.get, 'test_key')
