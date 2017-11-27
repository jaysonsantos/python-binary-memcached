import os
import unittest

import six

import bmemcached
from bmemcached.exceptions import MemcachedException

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
