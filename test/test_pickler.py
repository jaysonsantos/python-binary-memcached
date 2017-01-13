import json
import os

from io import BytesIO

try:
    import cPickle as pickle
except ImportError:
    import pickle
import unittest

import bmemcached


class PickleableThing(object):
    pass


class JsonPickler(object):
    def __init__(self, f, protocol=0):
        self.f = f

    def dump(self, obj):
        if isinstance(self.f, BytesIO):
            return self.f.write(json.dumps(obj).encode())

        return json.dump(obj, self.f)

    def load(self):
        if isinstance(self.f, BytesIO):
            return json.loads(self.f.read().decode())

        return json.load(self.f)


class PicklerTests(unittest.TestCase):
    def setUp(self):
        self.server = '{}:11211'.format(os.environ['MEMCACHED_HOST'])
        self.json_client = bmemcached.Client(self.server, 'user', 'password', pickler=JsonPickler,
                                             unpickler=JsonPickler)
        self.pickle_client = bmemcached.Client(self.server, 'user', 'password',
                                               pickler=pickle.Pickler,
                                               unpickler=pickle.Unpickler)
        self.data = {'a': 'b'}

    def tearDown(self):
        self.json_client.delete('test_key')
        self.json_client.disconnect_all()
        self.pickle_client.disconnect_all()

    def testPickleDict(self):
        self.pickle_client.set('test_key', self.data)
        self.assertEqual(self.data, self.pickle_client.get('test_key'))

    def testPickleClassInstance(self):
        to_pickle = PickleableThing()
        self.pickle_client.set('test_key', to_pickle)
        unpickled = self.pickle_client.get('test_key')
        self.assertEqual(type(unpickled), PickleableThing)
        self.assertFalse(unpickled is to_pickle)

    def testPickleVsJson(self):
        self.pickle_client.set('test_key', self.data)
        self.assertRaises(ValueError, self.json_client.get, 'test_key')

    def testJsonVsPickle(self):
        self.json_client.set('test_key', self.data)
        self.assertRaises(pickle.UnpicklingError, self.pickle_client.get, 'test_key')
