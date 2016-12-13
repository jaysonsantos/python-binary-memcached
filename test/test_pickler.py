
try:
    import cPickle as pickle
except ImportError:
    import pickle
import unittest
import bmemcached


class PickleableThing(object):
    pass


class PicklerTests(unittest.TestCase):

    def setUp(self):
        self.server = '127.0.0.1:11211'
        self.json_client = bmemcached.Client(self.server, 'user', 'password')
        self.pickle_client = bmemcached.Client(self.server, 'user', 'password',
                                               dumps=pickle.dumps,
                                               loads=pickle.loads)
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
