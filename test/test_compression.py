import bz2
import os
import unittest

import six

import bmemcached

if six.PY3:
    from unittest import mock
else:
    import mock


class MemcachedTests(unittest.TestCase):
    def setUp(self):
        self.server = '{}:11211'.format(os.environ['MEMCACHED_HOST'])
        self.client = bmemcached.Client(self.server, 'user', 'password')
        self.bzclient = bmemcached.Client(self.server, 'user', 'password',
                                          compression=bz2)
        self.data = 'this is test data. ' * 32

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

    def testCompressionEnabled(self):
        import zlib
        compression = mock.Mock()
        compression.compress.side_effect = zlib.compress
        compression.decompress.side_effect = zlib.decompress
        for proto in self.client._servers:
            proto.compression = compression
        self.client.set('test_key', self.data)
        self.assertEqual(self.data, self.client.get('test_key'))
        compression.compress.assert_called_with(self.data.encode('ascii'))
        self.assertEqual(1, compression.decompress.call_count)

    def testCompressionDisabled(self):
        compression = mock.Mock()
        for proto in self.client._servers:
            proto.compression = compression
        self.client.set('test_key', self.data, compress_level=0)
        self.assertEqual(self.data, self.client.get('test_key'))
        compression.compress.assert_not_called()
        compression.decompress.assert_not_called()
