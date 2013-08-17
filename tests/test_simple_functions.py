import unittest
import bmemcached

try:
    long
except NameError:
    long = int

class MemcachedTests(unittest.TestCase):
    def setUp(self):
        self.server = '127.0.0.1:11211'
        self.client = bmemcached.Client(self.server, 'user', 'password')

    def tearDown(self):
        self.client.delete(b'test_key')
        self.client.delete(b'test_key2')
        self.client.disconnect_all()

    def testSet(self):
        self.assertTrue(self.client.set(b'test_key', b'test'))

    def testSetMulti(self):
        self.assertTrue(self.client.set_multi({
            b'test_key': b'value',
            b'test_key2': b'value2'}))

    def testGet(self):
        self.client.set(b'test_key', b'test')
        self.assertEqual(b'test', self.client.get(b'test_key'))

    def testGetEmptyString(self):
        self.client.set(b'test_key', b'')
        self.assertEqual(b'', self.client.get(b'test_key'))

    def testGetMulti(self):
        self.assertTrue(self.client.set_multi({
            b'test_key': b'value',
            b'test_key2': b'value2'
        }))
        self.assertEqual({b'test_key': b'value', b'test_key2': b'value2'},
                         self.client.get_multi([b'test_key', b'test_key2']))
        self.assertEqual({b'test_key': b'value', b'test_key2': b'value2'},
                         self.client.get_multi([b'test_key', b'test_key2', b'nothere']))

    def testGetLong(self):
        self.client.set(b'test_key', long(1))
        value = self.client.get(b'test_key')
        self.assertEqual(long(1), value)
        self.assertTrue(isinstance(value, long))

    def testGetInteger(self):
        self.client.set(b'test_key', 1)
        value = self.client.get(b'test_key')
        self.assertEqual(1, value)
        self.assertTrue(isinstance(value, int))

    def testGetObject(self):
        self.client.set(b'test_key', {'a': 1})
        value = self.client.get(b'test_key')
        self.assertTrue(isinstance(value, dict))
        self.assertTrue('a' in value)
        self.assertEqual(1, value['a'])

    def testDelete(self):
        self.client.set(b'test_key', b'test')
        self.assertTrue(self.client.delete(b'test_key'))
        self.assertEqual(None, self.client.get(b'test_key'))

    def testDeleteUnknownKey(self):
        self.assertTrue(self.client.delete(b'test_key'))

    def testAddPass(self):
        self.assertTrue(self.client.add(b'test_key', b'test'))

    def testAddFail(self):
        self.client.add(b'test_key', b'value')
        self.assertFalse(self.client.add(b'test_key', b'test'))

    def testReplacePass(self):
        self.client.add(b'test_key', b'value')
        self.assertTrue(self.client.replace(b'test_key', b'value2'))
        self.assertEqual(b'value2', self.client.get(b'test_key'))

    def testReplaceFail(self):
        self.assertFalse(self.client.replace(b'test_key', b'value'))

    def testIncrement(self):
        self.assertEqual(0, self.client.incr(b'test_key', 1))
        self.assertEqual(1, self.client.incr(b'test_key', 1))

    def testDecrement(self):
        self.assertEqual(0, self.client.decr(b'test_key', 1))
        self.assertEqual(0, self.client.decr(b'test_key', 1))

    def testFlush(self):
        self.client.set(b'test_key', b'test')
        self.assertTrue(self.client.flush_all())
        self.assertEqual(None, self.client.get(b'test_key'))

    def testStats(self):
        stats = self.client.stats()[self.server]
        self.assertTrue(b'pid' in stats)

        stats = self.client.stats(b'settings')[self.server]
        self.assertTrue(b'verbosity' in stats)

        stats = self.client.stats(b'slabs')[self.server]
        self.assertTrue(b'1:get_hits' in stats)
