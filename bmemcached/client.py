import logging
import itertools
from hash_ring.hash_ring import HashRing
from bmemcached.protocol import Protocol

try:
    import cPickle as pickle
except ImportError:
    import pickle


from bmemcached.protocol import Protocol


_SOCKET_TIMEOUT = 3


class Client(object):
    """
    This is intended to be a client class which implement standard cache interface that common libs do.
    """
    def __init__(self, servers=('127.0.0.1:11211',), username=None,
                 password=None, compression=None,
                 socket_timeout=_SOCKET_TIMEOUT,
                 pickleProtocol=0,
                 pickler=pickle.Pickler, unpickler=pickle.Unpickler):
        """
        :param servers: A list of servers with ip[:port] or unix socket.
        :type servers: list
        :param username: If your server have auth activated, provide it's username.
        :type username: basestring
        :param password: If your server have auth activated, provide it's password.
        :type password: basestring
        """
        self.username = username
        self.password = password
        self.compression = compression
        self.socket_timeout = socket_timeout
        self.pickleProtocol = pickleProtocol
        self.pickler = pickler
        self.unpickler = unpickler
        self.set_servers(servers)

    @property
    def servers(self):
        for server in self._servers.nodes:
            yield server

    def get_server(self, key):
        return self._servers.get_node(key)

    def set_servers(self, servers):
        """
        Iter to a list of servers and instantiate Protocol class.

        :param servers: A list of servers
        :type servers: list
        :return: Returns nothing
        :rtype: None
        """
        if isinstance(servers, basestring):
            servers = [servers]

        assert servers, "No memcached servers supplied"
        self._servers = HashRing([Protocol(server,
                                           self.username,
                                           self.password,
                                           self.compression,
                                           self.socket_timeout,
                                           self.pickleProtocol,
                                           self.pickler,
                                           self.unpickler) for server in servers])

    def _set_retry_delay(self, value):
        for server in self._servers:
            server.set_retry_delay(value)

    def enable_retry_delay(self, enable):
        """
        Enable or disable delaying between reconnection attempts.

        The first reconnection attempt will always happen immediately, so intermittent network
        errors don't cause caching to turn off.  The retry delay takes effect after the first
        reconnection fails.

        The reconnection delay is enabled by default for TCP connections, and disabled by
        default for Unix socket connections.
        """
        # The public API only allows enabling or disabling the delay, so it'll be easier to
        # add exponential falloff in the future.  _set_retry_delay is exposed for tests.
        self._set_retry_delay(5 if enable else 0)

    def get(self, key, get_cas=False):
        """
        Get a key from server.

        :param key: Key's name
        :type key: basestring
        :param get_cas: If true, return (value, cas), where cas is the new CAS value.
        :type get_cas: boolean
        :return: Returns a key data from server.
        :rtype: object
        """
        value, cas = self.get_server(key).get(key)
        if get_cas:
            return value, cas

        return value

    def gets(self, key):
        """
        Get a key from server, returning the value and its CAS key.

        This method is for API compatibility with other implementations.

        :param key: Key's name
        :type key: basestring
        :return: Returns (key data, value), or (None, None) if the value is not in cache.
        :rtype: object
        """
        value, cas = self.get(key, True)

        if value is not None:
            return value, cas

        return None, None

    def get_multi(self, keys, get_cas=False):
        """
        Get multiple keys from server.

        :param keys: A list of keys to from server.
        :type keys: list
        :param get_cas: If get_cas is true, each value is (data, cas), with each result's CAS value.
        :type get_cas: boolean
        :return: A dict with all requested keys.
        :rtype: dict
        """

        result = {}
        for server, keys in itertools.groupby(keys, key=self.get_server):
            for key, value_and_cas in server.get_multi(list(keys)).iteritems():
                # Protocol#get_multi returns both value and cas, so we need to discard the cas
                result[key] = value_and_cas[0]

        return result

    def set(self, key, value, time=0):
        """
        Set a value for a key on server.

        :param key: Key's name
        :type key: basestring
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        return self.get_server(key).set(key, value, time)

    def cas(self, key, value, cas, time=0):
        """
        Set a value for a key on server if its CAS value matches cas.

        :param key: Key's name
        :type key: basestring
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        return self.get_server(key).cas(key, value, cas, time) is not None

    def set_multi(self, mappings, time=0):
        """
        Set multiple keys with it's values on server.

        :param mappings: A dict with keys/values
        :type mappings: dict
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """

        returns = []
        for server, keys in itertools.groupby(mappings.iterkeys(), key=self.get_server):
            returns.append(server.set_multi({k: mappings[k] for k in keys}, time=time))

        return all(returns)

    def add(self, key, value, time=0):
        """
        Add a key/value to server ony if it does not exist.

        :param key: Key's name
        :type key: basestring
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True if key is added False if key already exists
        :rtype: bool
        """
        return self.get_server(key).add(key, value, time)

    def replace(self, key, value, time=0):
        """
        Replace a key/value to server ony if it does exist.

        :param key: Key's name
        :type key: basestring
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True if key is replace False if key does not exists
        :rtype: bool
        """
        return self.get_server(key).replace(key, value, time)

    def delete(self, key, cas=0):
        """
        Delete a key/value from server. If key does not exist, it returns True.

        :param key: Key's name to be deleted
        :type key: basestring
        :return: True in case o success and False in case of failure.
        :rtype: bool
        """
        return self.get_server(key).delete(key)

    def delete_multi(self, keys):
        returns = []
        for server, keys in itertools.groupby(keys, key=self.get_server):
            returns.append(server.delete_multi(list(keys)))

        return all(returns)

    def incr(self, key, value):
        """
        Increment a key, if it exists, returns it's actual value, if it don't, return 0.

        :param key: Key's name
        :type key: basestring
        :param value: Number to be incremented
        :type value: int
        :return: Actual value of the key on server
        :rtype: int
        """
        return self.get_server(key).incr(key, value)

    def decr(self, key, value):
        """
        Decrement a key, if it exists, returns it's actual value, if it don't, return 0.
        Minimum value of decrement return is 0.

        :param key: Key's name
        :type key: basestring
        :param value: Number to be decremented
        :type value: int
        :return: Actual value of the key on server
        :rtype: int
        """
        return self.get_server(key).decr(key, value)

    def flush_all(self, time=0):
        """
        Send a command to server flush|delete all keys.

        :param time: Time to wait until flush in seconds.
        :type time: int
        :return: True in case of success, False in case of failure
        :rtype: bool
        """
        returns = []
        for server in self.servers:
            returns.append(server.flush_all(time))

        return any(returns)

    def stats(self, key=None):
        """
        Return server stats.

        :param key: Optional if you want status from a key.
        :type key: basestring
        :return: A dict with server stats
        :rtype: dict
        """
        # TODO: Stats with key is not working.

        returns = {}
        for server in self.servers:
            returns[server.server] = server.stats(key)

        return returns

    def disconnect_all(self):
        """
        Disconnect all servers.

        :return: Nothing
        :rtype: None
        """
        for server in self.servers:
            server.disconnect()
