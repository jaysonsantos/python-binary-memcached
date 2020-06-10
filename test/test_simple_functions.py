import os
import unittest

import six
import struct
import random
import bmemcached
import uuid
from bmemcached.compat import long, unicode

if six.PY3:
    from unittest import mock
else:
    import mock


class MemcachedTests(unittest.TestCase):
    def setUp(self):
        self.server = '/tmp/memcached.sock'
        self.client = bmemcached.Client(self.server, 'user', 'password')
        self.reset()

    def tearDown(self):
        self.reset()
        self.client.disconnect_all()

    def reset(self):
        self.client.delete('test_key')
        self.client.delete('test_key2')

    def testSet(self):
        self.assertTrue(self.client.set('test_key', 'test'))

    def testSetMulti(self):
        six.assertCountEqual(self, self.client.set_multi({
            'test_key': 'value',
            'test_key2': 'value2'}), [])

    def testSetMultiBigData(self):
        self.client.set_multi(
            dict((unicode(k), b'value') for k in range(32767)))

    def testGetSimple(self):
        self.client.set('test_key', 'test')
        self.assertEqual('test', self.client.get('test_key'))

    def testGetDefault(self):
        self.assertEqual(None, self.client.get('test_key'))
        self.assertEqual('default_value', self.client.get('test_key', 'default_value'))

    def testGetBytes(self):
        # Ensure the code is 8-bit clean.
        value = b'\x01z\x7f\x00\x80\xfe\xff\x00'
        self.client.set('test_key', value)
        self.assertEqual(value, self.client.get('test_key'))

    def testGetDecodedText(self):
        self.client.set('test_key', u'\u30b7')
        self.assertEqual(u'\u30b7', self.client.get('test_key'))

    def testCas(self):
        value, cas = self.client.gets('nonexistant')
        self.assertTrue(value is None)
        self.assertTrue(cas is None)

        # cas() with a cas value of None is equivalent to add.
        self.assertTrue(self.client.cas('test_key', 'test', cas))
        self.assertFalse(self.client.cas('test_key', 'testX', cas))

        # Load the CAS key.
        value, cas = self.client.gets('test_key')
        self.assertEqual('test', value)
        self.assertTrue(cas is not None)

        # Overwrite test_key only if it hasn't changed since we read it.
        self.assertTrue(self.client.cas('test_key', 'test2', cas))
        self.assertEqual(self.client.get('test_key'), 'test2')

        # This call won't overwrite the value, since the CAS key is out of date.
        self.assertFalse(self.client.cas('test_key', 'test3', cas))
        self.assertEqual(self.client.get('test_key'), 'test2')

    def testCasDelete(self):
        self.assertTrue(self.client.set('test_key', 'test'))
        value, cas = self.client.gets('test_key')

        # If a different CAS value is supplied, the key is not deleted.
        self.assertFalse(self.client.delete('test_key', cas=cas + 1))
        self.assertEqual('test', self.client.get('test_key'))

        # If the correct CAS value is supplied, the key is deleted.
        self.assertTrue(self.client.delete('test_key', cas=cas))
        self.assertEqual(None, self.client.get('test_key'))

    def testMultiCas(self):
        # Set multiple values, some using CAS and some not.  True is returned, because
        # both values were stored.
        six.assertCountEqual(self, self.client.set_multi({
            ('test_key', 0): 'value1',
            'test_key2': 'value2',
        }), [])

        self.assertEqual(self.client.get('test_key'), 'value1')
        self.assertEqual(self.client.get('test_key2'), 'value2')

        # A CAS value of 0 means add.  The value already exists, so this won't overwrite it.
        # ['test_key'] is returned, because test_key is not stored, but test_key2 is still stored.
        six.assertCountEqual(self, self.client.set_multi({
            ('test_key', 0): 'value3',
            'test_key2': 'value3',
        }), [('test_key', 0)])

        self.assertEqual(self.client.get('test_key'), 'value1')
        self.assertEqual(self.client.get('test_key2'), 'value3')

        # Update with the correct CAS value.
        value, cas = self.client.gets('test_key')
        six.assertCountEqual(self, self.client.set_multi({
            ('test_key', cas): 'value4',
        }), [])
        self.assertEqual(self.client.get('test_key'), 'value4')

    def testGetMultiCas(self):
        self.client.set('test_key', 'value1')
        self.client.set('test_key2', 'value2')

        value1, cas1 = self.client.gets('test_key')
        value2, cas2 = self.client.gets('test_key2')

        # Batch retrieve items and their CAS values, and verify that they match
        # the values we got by looking them up individually.
        values = self.client.get_multi(['test_key', 'test_key2'], get_cas=True)
        self.assertEqual(values.get('test_key')[0], 'value1')
        self.assertEqual(values.get('test_key2')[0], 'value2')

    def testGetEmptyString(self):
        self.client.set('test_key', '')
        self.assertEqual('', self.client.get('test_key'))

    def testGetUnicodeString(self):
        self.client.set('test_key', u'\xac')
        self.assertEqual(u'\xac', self.client.get('test_key'))

    def testGetMulti(self):
        six.assertCountEqual(self, self.client.set_multi({
            'test_key': 'value',
            'test_key2': 'value2'
        }), [])
        self.assertEqual({'test_key': 'value', 'test_key2': 'value2'},
                         self.client.get_multi(['test_key', 'test_key2']))
        self.assertEqual({'test_key': 'value', 'test_key2': 'value2'},
                         self.client.get_multi(['test_key', 'test_key2', 'nothere']))

    def testGetLong(self):
        self.client.set('test_key', long(1))
        value = self.client.get('test_key')
        self.assertEqual(long(1), value)
        self.assertTrue(isinstance(value, long))

    def testGetInteger(self):
        self.client.set('test_key', 1)
        value = self.client.get('test_key')
        self.assertEqual(1, value)
        self.assertTrue(isinstance(value, int))

    def testGetBoolean(self):
        self.client.set('test_key', True)
        self.assertTrue(self.client.get('test_key') is True)

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

    def testDeleteMulti(self):
        self.client.set_multi({
            'test_key': 'value',
            'test_key2': 'value2'})

        self.assertTrue(self.client.delete_multi(['test_key', 'test_key2']))

    def testDeleteUnknownKey(self):
        self.assertTrue(self.client.delete('test_key'))

    def testAddPass(self):
        self.assertTrue(self.client.add('test_key', 'test'))

    def testAddFail(self):
        self.client.add('test_key', 'value')
        self.assertFalse(self.client.add('test_key', 'test'))

    def testReplacePass(self):
        self.client.add('test_key', 'value')
        self.assertTrue(self.client.replace('test_key', 'value2'))
        self.assertEqual('value2', self.client.get('test_key'))

    def testReplaceFail(self):
        self.assertFalse(self.client.replace('test_key', 'value'))

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
        stats = self.client.stats()[self.server]
        self.assertTrue('pid' in stats)

        stats = self.client.stats('settings')[self.server]
        self.assertTrue('verbosity' in stats)

    def testReconnect(self):
        self.client.set('test_key', 'test')
        self.client.disconnect_all()
        self.assertEqual('test', self.client.get('test_key'))


class TimeoutMemcachedTests(unittest.TestCase):
    def setUp(self):
        self.server = '{}:11211'.format(os.environ['MEMCACHED_HOST'])
        self.client = None

    def tearDown(self):
        if self.client:
            self.client.disconnect_all()
        client = bmemcached.Client(self.server, 'user', 'password',
                                   socket_timeout=None)
        client.delete('timeout_key')
        client.delete('timeout_key_none')
        client.disconnect_all()

    def testTimeout(self):
        self.client = bmemcached.Client(self.server, 'user', 'password',
                                        socket_timeout=0.00000000000001)

        for proto in self.client._servers:
            # Set up a mock connection that gives the impression of
            # timing out in every recv() call.
            proto.connection = mock.Mock()
            proto.connection.recv.return_value = b''

        self.client.set('timeout_key', 'test')
        self.assertEqual(self.client.get('timeout_key'), None)

    def testTimeoutNone(self):
        self.client = bmemcached.Client(self.server, 'user', 'password',
                                        socket_timeout=None)
        self.client.set('test_key_none', 'test')
        self.assertEqual(self.client.get('test_key_none'), 'test')


class BinaryMemcachedTests(unittest.TestCase):
    def setUp(self):
        self.server = '/tmp/memcached.sock'
        self.client = bmemcached.Client(self.server, 'user', 'password')
        self._inserted_keys = list()

        self.reset()

    def tearDown(self):
        self.reset()
        self.client.disconnect_all()

    def bkey(self):
        packed = struct.pack("<Q", int("%s%s%s%s" % (random.randint(1000, 9999),
                                                     random.randint(1000, 9999),
                                                     random.randint(1000, 9999),
                                                     random.randint(1000, 9999))))
        self._inserted_keys.append(packed)
        return packed

    def skey(self):
        key = str(uuid.uuid4())[0:8]
        self._inserted_keys.append(key)
        return key

    def reset(self):
        for test_key in self._inserted_keys:
            self.client.delete(test_key)

    def testSet(self):
        self.assertTrue(self.client.set(self.bkey(), 'test'))
        self.assertTrue(self.client.set(self.skey(), 'test'))

    def testSetMulti(self):
        six.assertCountEqual(self, self.client.set_multi({
            self.bkey(): 'value',
            self.skey(): 'value2',
            self.bkey(): 'value3'}), [])

    def testSetMultiBigData(self):
        self.client.set_multi(
            dict((self.bkey(), b'value') for _ in range(32767)))
        self.client.set_multi(
            dict((self.skey(), b'value') for _ in range(32767)))

    def testGetSimple(self):
        key = self.bkey()
        self.client.set(key, 'test')
        self.assertEqual('test', self.client.get(key))
        key = self.skey()
        self.client.set(key, 'test')
        self.assertEqual('test', self.client.get(key))

    def testGetBytes(self):
        test_key = self.bkey()
        # Ensure the code is 8-bit clean.
        value = b'\x01z\x7f\x00\x80\xfe\xff\x00'
        self.client.set(test_key, value)
        self.assertEqual(value, self.client.get(test_key))

    def testGetDecodedText(self):
        test_key = self.bkey()
        self.client.set(test_key, u'\u30b7')
        self.assertEqual(u'\u30b7', self.client.get(test_key))

    def testCas(self):
        value, cas = self.client.gets('nonexistant')
        self.assertTrue(value is None)
        self.assertTrue(cas is None)

        # cas() with a cas value of None is equivalent to add.
        test_key = self.bkey()
        self.assertTrue(self.client.cas(test_key, 'test', cas))
        self.assertFalse(self.client.cas(test_key, 'testX', cas))

        # Load the CAS key.
        value, cas = self.client.gets(test_key)
        self.assertEqual('test', value)
        self.assertTrue(cas is not None)

        # Overwrite test_key only if it hasn't changed since we read it.
        self.assertTrue(self.client.cas(test_key, 'test2', cas))
        self.assertEqual(self.client.get(test_key), 'test2')

        # This call won't overwrite the value, since the CAS key is out of date.
        self.assertFalse(self.client.cas(test_key, 'test3', cas))
        self.assertEqual(self.client.get(test_key), 'test2')

    def testCasDelete(self):
        test_key = self.bkey()
        self.assertTrue(self.client.set(test_key, 'test'))
        value, cas = self.client.gets(test_key)

        # If a different CAS value is supplied, the key is not deleted.
        self.assertFalse(self.client.delete(test_key, cas=cas + 1))
        self.assertEqual('test', self.client.get(test_key))

        # If the correct CAS value is supplied, the key is deleted.
        self.assertTrue(self.client.delete(test_key, cas=cas))
        self.assertEqual(None, self.client.get(test_key))

    def testMultiCas(self):
        # Set multiple values, some using CAS and some not.  True is returned, because
        # both values were stored.
        test_key1 = self.bkey()
        test_key2 = self.bkey()
        six.assertCountEqual(self, self.client.set_multi({
            (test_key1, 0): 'value1',
            test_key2: 'value2',
        }), [])

        self.assertEqual(self.client.get(test_key1), 'value1')
        self.assertEqual(self.client.get(test_key2), 'value2')

        # A CAS value of 0 means add.  The value already exists, so this won't overwrite it.
        # [test_key1] is returned, because test_key1 is not stored, but test_key2 is still stored.
        six.assertCountEqual(self, self.client.set_multi({
            (test_key1, 0): 'value3',
            test_key2: 'value3',
        }), [(test_key1, 0)])

        self.assertEqual(self.client.get(test_key1), 'value1')
        self.assertEqual(self.client.get(test_key2), 'value3')

        # Update with the correct CAS value.
        value, cas = self.client.gets(self.bkey())
        six.assertCountEqual(self, self.client.set_multi({
            (test_key1, cas): 'value4',
        }), [])
        self.assertEqual(self.client.get(test_key1), 'value4')

    def testGetMultiCas(self):
        for _ in range(0, 100):
            test_key1 = self.bkey()
            test_key2 = self.bkey()
            test_key3 = self.skey()
            self.client.set(test_key1, 'value1')
            self.client.set(test_key2, 'value2')

            value1, cas1 = self.client.gets(test_key1)
            value2, cas2 = self.client.gets(test_key2)

            # Batch retrieve items and their CAS values, and verify that they match
            # the values we got by looking them up individually.
            values = self.client.get_multi([test_key1, test_key2, test_key3], get_cas=True)
            self.assertEqual(values.get(test_key1)[0], 'value1')
            self.assertEqual(values.get(test_key2)[0], 'value2')

    def testGetEmptyString(self):
        test_key = self.bkey()
        self.client.set(test_key, '')
        self.assertEqual('', self.client.get(test_key))

    def testGetUnicodeString(self):
        test_key = self.bkey()
        self.client.set(test_key, u'\xac')
        self.assertEqual(u'\xac', self.client.get(test_key))

    def testGetMulti(self):
        test_key1 = self.bkey()
        test_key2 = self.bkey()
        test_key3 = self.skey()
        test_key4 = self.skey()
        six.assertCountEqual(self, self.client.set_multi({
            test_key1: 'value',
            test_key2: 'value2',
            test_key3: 'value3',
            test_key4: 'value4'

        }), [])
        self.assertEqual({test_key1: 'value', test_key2: 'value2', test_key3: 'value3'},
                         self.client.get_multi([test_key1, test_key2, test_key3]))
        self.assertEqual({test_key1: 'value', test_key2: 'value2', test_key3: 'value3', test_key4: 'value4'},
                         self.client.get_multi([test_key1, test_key2, test_key3, test_key4]))
        self.assertEqual({test_key1: 'value', test_key2: 'value2'},
                         self.client.get_multi([test_key1, test_key2, 'nothere']))

    def testGetLong(self):
        test_key = self.bkey()
        self.client.set(test_key, long(1))
        value = self.client.get(test_key)
        self.assertEqual(long(1), value)
        self.assertTrue(isinstance(value, long))

    def testGetInteger(self):
        test_key = self.bkey()
        self.client.set(test_key, 1)
        value = self.client.get(test_key)
        self.assertEqual(1, value)
        self.assertTrue(isinstance(value, int))

    def testGetBoolean(self):
        test_key = self.bkey()
        self.client.set(test_key, True)
        self.assertTrue(self.client.get(test_key) is True)

    def testGetObject(self):
        test_key = self.bkey()
        self.client.set(test_key, {'a': 1})
        value = self.client.get(test_key)
        self.assertTrue(isinstance(value, dict))
        self.assertTrue('a' in value)
        self.assertEqual(1, value['a'])

    def testDelete(self):
        test_key = self.bkey()
        self.client.set(test_key, 'test')
        self.assertTrue(self.client.delete(test_key))
        self.assertEqual(None, self.client.get(test_key))

    def testDeleteMulti(self):
        test_key1 = self.bkey()
        test_key2 = self.bkey()
        self.client.set_multi({
            test_key1: 'value',
            test_key2: 'value2'})
        self.assertTrue(self.client.delete_multi([test_key1, test_key2]))

    def testDeleteUnknownKey(self):
        test_key = self.bkey()
        self.assertTrue(self.client.delete(test_key))

    def testAddPass(self):
        test_key = self.bkey()
        self.assertTrue(self.client.add(test_key, 'test'))

    def testAddFail(self):
        test_key = self.bkey()
        self.client.add(test_key, 'value')
        self.assertFalse(self.client.add(test_key, 'test'))

    def testReplacePass(self):
        test_key = self.bkey()
        self.client.add(test_key, 'value')
        self.assertTrue(self.client.replace(test_key, 'value2'))
        self.assertEqual('value2', self.client.get(test_key))

    def testReplaceFail(self):
        test_key = self.bkey()
        self.assertFalse(self.client.replace(test_key, 'value'))

    def testIncrement(self):
        test_key = self.bkey()
        self.assertEqual(0, self.client.incr(test_key, 1))
        self.assertEqual(1, self.client.incr(test_key, 1))

    def testDecrement(self):
        test_key = self.bkey()
        self.assertEqual(0, self.client.decr(test_key, 1))
        self.assertEqual(0, self.client.decr(test_key, 1))

    def testFlush(self):
        test_key = self.bkey()
        self.client.set(test_key, 'test')
        self.assertTrue(self.client.flush_all())
        self.assertEqual(None, self.client.get(test_key))

    def testStats(self):
        stats = self.client.stats()[self.server]
        self.assertTrue('pid' in stats)

        stats = self.client.stats('settings')[self.server]
        self.assertTrue('verbosity' in stats)

    def testReconnect(self):
        test_key = self.bkey()
        self.client.set(test_key, 'test')
        self.client.disconnect_all()
        self.assertEqual('test', self.client.get(test_key))


class DistributedClient(MemcachedTests):
    def setUp(self):
        self.server = '{}:11211'.format(os.environ['MEMCACHED_HOST'])
        self.client = bmemcached.DistributedClient([self.server], 'user', 'password')
        self.reset()
