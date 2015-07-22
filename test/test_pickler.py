try:
    import cPickle as pickle
except ImportError:
    import pickle
import json
import unittest
import bmemcached


class JsonPickler(object):

    def __init__(self, f, protocol=0):
        self.f = f

    def dump(self, obj):
        return json.dump(obj, self.f)

    def load(self):
        return json.load(self.f)


class MemcachedTests(unittest.TestCase):

    def setUp(self):
        self.server = '127.0.0.1:11211'
        self.dclient = bmemcached.Client(self.server, 'user', 'password')
        self.jclient = bmemcached.Client(self.server, 'user', 'password',
                                         pickler=JsonPickler,
                                         unpickler=JsonPickler)
        self.data = {'a': 'b'}

    def tearDown(self):
        self.jclient.delete('test_key')
        self.jclient.disconnect_all()
        self.dclient.disconnect_all()

    def testJson(self):
        self.jclient.set('test_key', self.data)
        self.assertEqual(self.data, self.jclient.get('test_key'))

    def testDefaultVsJson(self):
        self.dclient.set('test_key', self.data)
        self.assertRaises(ValueError, self.jclient.get, 'test_key')

    def testJsonVsDefault(self):
        self.jclient.set('test_key', self.data)
        self.assertRaises(pickle.UnpicklingError, self.dclient.get, 'test_key')
