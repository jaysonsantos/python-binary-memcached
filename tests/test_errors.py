import unittest
import mock
import bmemcached
from bmemcached.exceptions import MemcachedException


class TestMemcachedErrors(unittest.TestCase):
    def testGet(self):
        """
        Raise MemcachedException if request wasn't successful and
        wasn't a 'key not found' error.
        """
        client = bmemcached.Client('127.0.0.1:11211', 'user', 'password')
        with mock.patch.object(bmemcached.client.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.get, b'foo')

    def testSet(self):
        """
        Raise MemcachedException if request wasn't successful and
        wasn't a 'key not found' or 'key exists' error.
        """
        client = bmemcached.Client('127.0.0.1:11211', 'user', 'password')
        with mock.patch.object(bmemcached.client.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.set, b'foo', b'bar', 300)

    def testIncrDecr(self):
        """
        Incr/Decr raise MemcachedException unless the request wasn't
        successful.
        """
        client = bmemcached.Client('127.0.0.1:11211', 'user', 'password')
        client.set(b'foo', 1)
        with mock.patch.object(bmemcached.client.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 2)
            self.assertRaises(MemcachedException, client.incr, b'foo', 1)
            self.assertRaises(MemcachedException, client.decr, b'foo', 1)

    def testDelete(self):
        """
        Raise MemcachedException if the delete request isn't successful.
        """
        client = bmemcached.Client('127.0.0.1:11211', 'user', 'password')
        client.flush_all()
        with mock.patch.object(bmemcached.client.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.delete, b'foo')

    def testFlushAll(self):
        """
        Raise MemcachedException if the flush wasn't successful.
        """
        client = bmemcached.Client('127.0.0.1:11211', 'user', 'password')
        with mock.patch.object(bmemcached.client.Protocol, '_get_response') as mocked_response:
            mocked_response.return_value = (0, 0, 0, 0, 0, 0x81, 0, 0, 0, 0)
            self.assertRaises(MemcachedException, client.flush_all)

