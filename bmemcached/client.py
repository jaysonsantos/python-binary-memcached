import logging
from bmemcached.protocol import Protocol

try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps


class Client(object):
    """
    This is intended to be a client class which implement standard cache interface that common libs do.
    """
    def __init__(self, servers=['127.0.0.1:11211'], username=None,
                 password=None, compression=None):
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
        self.set_servers(servers)


    @property
    def servers(self):
        for server in self._servers:
            yield server

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
        self._servers = [Protocol(server, self.username, self.password,
                                  self.compression) for server in servers]

    def get(self, key):
        """
        Get a key from server.

        :param key: Key's name
        :type key: basestring
        :return: Returns a key data from server.
        :rtype: object
        """
        for server in self.servers:
            value = server.get(key)
            if value is not None:
                return value

    def get_multi(self, keys):
        """
        Get multiple keys from server.

        :param keys: A list of keys to from server.
        :type keys: list
        :return: A dict with all requested keys.
        :rtype: dict
        """
        d = {}
        if keys:
            for server in self.servers:
                d.update(server.get_multi(keys))
                keys = [_ for _ in keys if not _ in d]
                if not keys:
                    break
        return d

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
        returns = []
        for server in self.servers:
            returns.append(server.set(key, value, time))

        return any(returns)

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
        if mappings:
            for server in self.servers:
                returns.append(server.set_multi(mappings, time))

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
        returns = []
        for server in self.servers:
            returns.append(server.add(key, value, time))

        return any(returns)

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
        returns = []
        for server in self.servers:
            returns.append(server.replace(key, value, time))

        return any(returns)

    def delete(self, key):
        """
        Delete a key/value from server. If key does not exist, it returns True.

        :param key: Key's name to be deleted
        :type key: basestring
        :return: True in case o success and False in case of failure.
        :rtype: bool
        """
        returns = []
        for server in self.servers:
            returns.append(server.delete(key))

        return any(returns)

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
        returns = []
        for server in self.servers:
            returns.append(server.incr(key, value))

        return returns[0]

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
        returns = []
        for server in self.servers:
            returns.append(server.decr(key, value))

        return returns[0]

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
