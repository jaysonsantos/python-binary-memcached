import logging
from bmemcached.server import Server

try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps

logger = logging.getLogger(__name__)


class Client(object):
    def __init__(self, servers=['127.0.0.1:11211'], username=None, password=None):
        self.username = username
        self.password = password
        self.set_servers(servers)

    def set_servers(self, servers):
        if isinstance(servers, basestring):
            servers = [servers]

        assert servers, "No memcached servers supplied"

        self.servers = [Server(server, self.username,
                               self.password) for server in servers]

    def get(self, key):
        for server in self.servers:
            value = server.get(key)
            if value is not None:
                return value

    def get_multi(self, keys):
        d = {}
        if keys:
            for server in self.servers:
                d.update(server.get_multi(keys))
                keys = [_ for _ in keys if not _ in d]
                if not keys:
                    break
        return d

    def set(self, key, value, time=100):
        returns = []
        for server in self.servers:
            returns.append(server.set(key, value, time))

        return any(returns)

    def set_multi(self, mappings, time=100):
        returns = []
        if mappings:
            for server in self.servers:
                returns.append(server.set_multi(mappings, time))

        return all(returns)

    def add(self, key, value, time=100):
        returns = []
        for server in self.servers:
            returns.append(server.add(key, value, time))

        return any(returns)

    def replace(self, key, value, time=100):
        returns = []
        for server in self.servers:
            returns.append(server.replace(key, value, time))

        return any(returns)

    def delete(self, key):
        returns = []
        for server in self.servers:
            returns.append(server.delete(key))

        return any(returns)

    def incr(self, key, value):
        returns = []
        for server in self.servers:
            returns.append(server.incr(key, value))

        return returns[0]

    def decr(self, key, value):
        returns = []
        for server in self.servers:
            returns.append(server.decr(key, value))

        return returns[0]

    def flush_all(self, time=0):
        returns = []
        for server in self.servers:
            returns.append(server.flush_all(time))

        return any(returns)

    def stats(self, key=None):
        returns = {}
        for server in self.servers:
            returns[server.server] = server.stats(key)

        return returns

    def disconnect_all(self):
        for server in self.servers:
            server.disconnect()

