import os
import unittest
import warnings

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
        self.client.delete('fresh_key')

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

    def testSetMultiCas(self):
        # All-success plain keys: every input gets a non-None CAS, and each
        # returned CAS matches what gets() reports afterwards.
        result = self.client.set_multi_cas({
            'test_key': 'value1',
            'test_key2': 'value2',
        })
        self.assertEqual(set(result.keys()), {'test_key', 'test_key2'})
        self.assertTrue(result['test_key'] is not None)
        self.assertTrue(result['test_key2'] is not None)
        _, cas1 = self.client.gets('test_key')
        _, cas2 = self.client.gets('test_key2')
        self.assertEqual(result['test_key'], cas1)
        self.assertEqual(result['test_key2'], cas2)

        # CAS failure: add-if-not-exists when the key already exists returns
        # None for that key; unrelated keys still succeed.
        result = self.client.set_multi_cas({
            ('test_key', 0): 'shouldnt_store',
            'fresh_key': 'fresh',
        })
        self.assertTrue(result['test_key'] is None)
        self.assertTrue(result['fresh_key'] is not None)
        self.assertEqual(self.client.get('test_key'), 'value1')
        self.client.delete('fresh_key')

        # Stale-CAS failure: capture cas, mutate out of band, then set_multi_cas
        # with the stale cas must fail and leave the out-of-band value intact.
        _, stale_cas = self.client.gets('test_key')
        self.client.set('test_key', 'other')
        result = self.client.set_multi_cas({
            ('test_key', stale_cas): 'should_fail',
        })
        self.assertTrue(result['test_key'] is None)
        self.assertEqual(self.client.get('test_key'), 'other')

        # Returned CAS is usable directly in cas() without a gets() round-trip.
        self.client.delete('test_key')
        result = self.client.set_multi_cas({'test_key': 'v'})
        self.assertTrue(self.client.cas('test_key', 'v2', result['test_key']))
        self.assertEqual(self.client.get('test_key'), 'v2')

    def testSetMultiCasEmpty(self):
        self.assertEqual(self.client.set_multi_cas({}), {})

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

    def testCasMultiReplicaWarns(self):
        # Pre-existing CAS-touching methods on ReplicatingClient produce
        # silently-wrong behavior when run against more than one replica
        # (each server has its own CAS counter). Confirm each fires a
        # UserWarning at runtime so callers have some signal that they
        # should reconfigure.
        client = bmemcached.Client(
            ['/tmp/memcached.sock', '{}:11211'.format(os.environ['MEMCACHED_HOST'])],
            'user', 'password',
        )
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                client.cas('test_key', 'v', None)
                client.gets('test_key')
                client.get('test_key', get_cas=True)
                client.get_multi(['test_key'], get_cas=True)
                client.set_multi({('test_key', 0): 'v'})
            messages = [str(w.message) for w in caught
                        if issubclass(w.category, UserWarning)]
            self.assertEqual(len(messages), 5)
            self.assertTrue(any('cas() on a ReplicatingClient' in m for m in messages))
            self.assertTrue(any('gets() on a ReplicatingClient' in m for m in messages))
            self.assertTrue(any('get(get_cas=True) on a ReplicatingClient' in m for m in messages))
            self.assertTrue(any('get_multi(get_cas=True) on a ReplicatingClient' in m for m in messages))
            self.assertTrue(any('set_multi() with (key, cas) tuple keys on a ReplicatingClient' in m for m in messages))

            # Non-CAS calls do not warn, even on multi-replica.
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                client.set('test_key', 'v')
                client.get('test_key')
                client.get_multi(['test_key'])
                client.set_multi({'test_key': 'v'})
            user_warnings = [w for w in caught
                             if issubclass(w.category, UserWarning)]
            self.assertEqual(user_warnings, [])
        finally:
            client.delete('test_key')
            client.disconnect_all()

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

    def testAddCas(self):
        success, cas = self.client.add('test_key', 'value', get_cas=True)
        self.assertTrue(success)
        self.assertTrue(cas is not None)

        # The CAS returned by add() must equal the CAS later returned by gets().
        _, gets_cas = self.client.gets('test_key')
        self.assertEqual(cas, gets_cas)

        # A second add of the same key fails; cas is None.
        success2, cas2 = self.client.add('test_key', 'value2', get_cas=True)
        self.assertFalse(success2)
        self.assertTrue(cas2 is None)

        # The CAS returned from add() can be used directly in cas() without
        # a separate gets() round-trip.
        self.assertTrue(self.client.cas('test_key', 'value3', cas))
        self.assertEqual('value3', self.client.get('test_key'))

        # Backward compatibility: with no get_cas kwarg, add() still returns a plain bool.
        result = self.client.add('test_key2', 'value')
        self.assertEqual(True, result)

    def testSetCas(self):
        # set() with get_cas=True returns (True, cas) and cas matches gets().
        success, cas = self.client.set('test_key', 'v1', get_cas=True)
        self.assertTrue(success)
        self.assertTrue(cas is not None)
        _, gets_cas = self.client.gets('test_key')
        self.assertEqual(cas, gets_cas)

        # The returned CAS is usable directly in cas() without a gets() round-trip.
        self.assertTrue(self.client.cas('test_key', 'v2', cas))
        self.assertEqual('v2', self.client.get('test_key'))

        # Backward compatibility: no get_cas kwarg still returns a plain bool.
        self.assertEqual(True, self.client.set('test_key2', 'v'))

    def testReplaceCas(self):
        # Replace on a nonexistent key fails; cas is None.
        success, cas = self.client.replace('test_key', 'v', get_cas=True)
        self.assertFalse(success)
        self.assertTrue(cas is None)

        # Replace on an existing key succeeds and returns the new CAS.
        self.client.set('test_key', 'original')
        success, cas = self.client.replace('test_key', 'new', get_cas=True)
        self.assertTrue(success)
        self.assertTrue(cas is not None)
        _, gets_cas = self.client.gets('test_key')
        self.assertEqual(cas, gets_cas)

        # Backward compatibility: no get_cas kwarg still returns a plain bool.
        self.assertEqual(True, self.client.replace('test_key', 'x'))

    def testCasCas(self):
        # cas() with get_cas=True, invoked as add (cas=None): returns new CAS.
        success, cas = self.client.cas('test_key', 'v1', None, get_cas=True)
        self.assertTrue(success)
        self.assertTrue(cas is not None)

        # Chain a second CAS using the returned value directly (no gets()).
        success2, cas2 = self.client.cas('test_key', 'v2', cas, get_cas=True)
        self.assertTrue(success2)
        self.assertTrue(cas2 is not None)
        self.assertNotEqual(cas, cas2)
        self.assertEqual('v2', self.client.get('test_key'))

        # A stale CAS fails; the returned new_cas is None.
        success3, cas3 = self.client.cas('test_key', 'v3', cas, get_cas=True)
        self.assertFalse(success3)
        self.assertTrue(cas3 is None)
        self.assertEqual('v2', self.client.get('test_key'))

        # Backward compatibility: no get_cas kwarg still returns a plain bool.
        self.assertEqual(True, self.client.cas('test_key', 'v4', cas2))

    def testReplacePass(self):
        self.client.add('test_key', 'value')
        self.assertTrue(self.client.replace('test_key', 'value2'))
        self.assertEqual('value2', self.client.get('test_key'))

    def testReplaceFail(self):
        self.assertFalse(self.client.replace('test_key', 'value'))

    def testIncrement(self):
        self.assertEqual(0, self.client.incr('test_key', 1))
        self.assertEqual(1, self.client.incr('test_key', 1))

    def testIncrementInitialize(self):
        self.assertEqual(10, self.client.incr('test_key', 1, default=10))
        self.assertEqual(11, self.client.incr('test_key', 1, default=10))

    def testDecrement(self):
        self.assertEqual(0, self.client.decr('test_key', 1))
        self.assertEqual(0, self.client.decr('test_key', 1))

    def testDecrementInitialize(self):
        self.assertEqual(10, self.client.decr('test_key', 1, default=10))
        self.assertEqual(9, self.client.decr('test_key', 1, default=10))

    def testNonAsciiKeySingle(self):
        key = u'シシ'
        try:
            self.assertEqual(0, self.client.incr(key, 1))
            self.assertEqual(1, self.client.incr(key, 1))
            self.assertEqual(0, self.client.decr(key, 1))
            self.client.delete(key)

            self.assertTrue(self.client.set(key, 'v1'))
            self.assertEqual('v1', self.client.get(key))

            self.assertFalse(self.client.add(key, 'v2'))
            self.assertTrue(self.client.replace(key, 'v3'))
            self.assertEqual('v3', self.client.get(key))

            value, cas = self.client.gets(key)
            self.assertEqual('v3', value)
            self.assertTrue(self.client.cas(key, 'v4', cas))
            self.assertEqual('v4', self.client.get(key))

            self.assertTrue(self.client.delete(key))
            self.assertEqual(None, self.client.get(key))
        finally:
            self.client.delete(key)

    def testSetLargeNumeric(self):
        big = 10 ** 200
        self.client.set('test_key', big)
        self.assertEqual(big, self.client.get('test_key'))

    def testNonAsciiKeyBulk(self):
        keys = [u'café', u'日本語']
        try:
            self.assertEqual([], self.client.set_multi({k: 'v' for k in keys}))
            self.assertEqual({k: 'v' for k in keys}, self.client.get_multi(keys))

            self.client.delete_multi(keys)
            self.assertEqual({}, self.client.get_multi(keys))

            result = self.client.set_multi_cas({k: 'w' for k in keys})
            for k in keys:
                self.assertTrue(result[k] is not None)
            self.assertEqual({k: 'w' for k in keys}, self.client.get_multi(keys))
        finally:
            for k in keys:
                self.client.delete(k)

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

    def testGetCasMultiReplicaRaises(self):
        # A ReplicatingClient with >1 server can't safely return a per-server
        # CAS, since each replica has its own CAS counter. Confirm every new
        # get_cas path raises NotImplementedError rather than returning a
        # value the caller can't use.
        client = bmemcached.Client(
            ['/tmp/memcached.sock', '{}:11211'.format(os.environ['MEMCACHED_HOST'])],
            'user', 'password',
        )
        try:
            with self.assertRaises(NotImplementedError):
                client.add('test_key', 'v', get_cas=True)
            with self.assertRaises(NotImplementedError):
                client.set('test_key', 'v', get_cas=True)
            with self.assertRaises(NotImplementedError):
                client.replace('test_key', 'v', get_cas=True)
            with self.assertRaises(NotImplementedError):
                client.cas('test_key', 'v', None, get_cas=True)
            with self.assertRaises(NotImplementedError):
                client.set_multi_cas({'test_key': 'v'})

            # get_cas=False (default) still works fine on multi-replica.
            self.assertTrue(client.set('test_key', 'v'))
        finally:
            client.delete('test_key')
            client.disconnect_all()


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
