import re
from urllib import splitport
import struct
import socket
import logging
import zlib

try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps

__all__ = ['Client']
logger = logging.getLogger('bmemcached')


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
        values = []
        for key in keys:
            for server in self.servers:
                value = server.get(key)
                if value is not None:
                    values.append((key, value))
                    break

        return dict(values)

    def set(self, key, value, time=100):
        returns = []
        for server in self.servers:
            returns.append(server.set(key, value, time))

        return any(returns)

    def set_multi(self, mappings, time=100):
        returns = []
        for key, value in mappings.iteritems():
            returns.append(self.set(key, value, time))

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


class Server(object):
    HEADER_STRUCT = '!BBHBBHLLQ'
    HEADER_SIZE = 24

    MAGIC = {
        'request': 0x80,
        'response': 0x81
    }

    # All structures will be appended to HEADER_STRUCT
    COMMANDS = {
        'get': {'command': 0x00, 'struct': '%ds'},
        'set': {'command': 0x01, 'struct': 'LL%ds%ds'},
        'add': {'command': 0x02, 'struct': 'LL%ds%ds'},
        'replace': {'command': 0x03, 'struct': 'LL%ds%ds'},
        'delete': {'command': 0x04, 'struct': '%ds'},
        'incr': {'command': 0x05, 'struct': 'QQL%ds'},
        'decr': {'command': 0x06, 'struct': 'QQL%ds'},
        'flush': {'command': 0x08, 'struct': 'I'},
        'stat': {'command': 0x10},
        'auth_negotiation': {'command': 0x20},
        'auth_request': {'command': 0x21, 'struct': '%ds%ds'}
    }

    STATUS = {
        'success': 0x00,
        'key_not_found': 0x01,
        'key_exists': 0x02,
        'auth_error': 0x08,
        'unknown_command': 0x81
    }

    FLAGS = {
        'pickle': 1 << 0,
        'integer': 1 << 1,
        'long': 1 << 2,
        'compressed': 1 << 3
    }

    def __init__(self, server, username=None, password=None):
        self.server = server
        self.authenticated = False

        if server.startswith('/'):
            self.connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.connection.connect(server)
        else:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.settimeout(5)
            self.host, self.port = self.split_host_port(self.server)
            self.connection.connect((self.host, self.port))

        if username and password:
            self.authenticate(username, password)

    def split_host_port(self, server):
        """
        Return (host, port) from server.

        Port defaults to 11211.

        >>> split_host_port('127.0.0.1:11211')
        ('127.0.0.1', 11211)
        >>> split_host_port('127.0.0.1')
        ('127.0.0.1', 11211)
        """
        host, port = splitport(server)
        if port is None:
            port = 11211
        port = int(port)
        if re.search(':.*$', host):
            host = re.sub(':.*$', '', host)
        return (host, port)

    def _read_socket(self, size):
        value = ''
        while len(value) < size:
            value += self.connection.recv(size - len(value))
        assert len(value) == size, "Asked for %d bytes, got %d" % (size, len(value))
        return value

    def _get_response(self):
        header = self._read_socket(self.HEADER_SIZE)
        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas) = struct.unpack(self.HEADER_STRUCT, header)

        assert magic == self.MAGIC['response']

        extra_content = None
        if bodylen:
            extra_content = self._read_socket(bodylen)

        return (magic, opcode, keylen, extlen, datatype, status, bodylen,
            opaque, cas, extra_content)

    def authenticate(self, username, password):
        logger.info('Authenticating as %s' % username)
        self.connection.send(struct.pack(self.HEADER_STRUCT,
            self.MAGIC['request'],
            self.COMMANDS['auth_negotiation']['command'],
            0, 0, 0, 0, 0, 0, 0))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        if status == self.STATUS['unknown_command']:
            logger.debug('Server does not requires authentication.')
            return True

        methods = extra_content

        if not 'PLAIN' in methods:
            raise AuthenticationNotSupported('This module only supports ' + \
                'PLAIN auth for now.')

        method = 'PLAIN'
        auth = '\x00%s\x00%s' % (username, password)
        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS['auth_request']['struct'] % (len(method), len(auth)),
            self.MAGIC['request'], self.COMMANDS['auth_request']['command'],
            len(method), 0, 0, 0, len(method) + len(auth), 0, 0, method, auth))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        if status == self.STATUS['auth_error']:
            raise InvalidCredentials("Incorrect username or password")

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d Message: %s' % (status,
                extra_content))

        logger.debug('Auth OK. Code: %d Message: %s' % (status,
            extra_content))

        self.authenticated = True
        return True

    def serialize(self, value):
        flags = 0
        if isinstance(value, str):
            pass
        elif isinstance(value, int):
            flags |= self.FLAGS['integer']
            value = str(value)
        elif isinstance(value, long):
            flags |= self.FLAGS['long']
            value = str(value)
        else:
            flags |= self.FLAGS['pickle']
            value = dumps(value)

        value = zlib.compress(value)
        flags |= self.FLAGS['compressed']
        return (flags, value)

    def deserialize(self, value, flags):
        if flags & self.FLAGS['compressed']: # pragma: no branch
            value = zlib.decompress(value)

        if flags & self.FLAGS['integer']:
            return int(value)
        elif flags & self.FLAGS['long']:
            return long(value)
        elif flags & self.FLAGS['pickle']:
            return loads(value)

        return value

    def get(self, key):
        logger.info('Getting key %s' % key)
        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS['get']['struct'] % (len(key)),
            self.MAGIC['request'],
            self.COMMANDS['get']['command'],
            len(key), 0, 0, 0, len(key), 0, 0, key))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        logger.debug('Value Length: %d. Body length: %d. Data type: %d' % (
            extlen, bodylen, datatype))

        if status != self.STATUS['success']:
            if status == self.STATUS['key_not_found']:
                logger.debug('Key not found. Message: %s' \
                    % extra_content)
                return None

            raise MemcachedException('Code: %d Message: %s' % (status,
                extra_content))

        flags, value = struct.unpack('!L%ds' % (bodylen - 4, ), extra_content)

        return self.deserialize(value, flags)

    def _set_add_replace(self, command, key, value, time):
        logger.info('Setting/adding/replacing key %s.' % key)
        flags, value = self.serialize(value)
        logger.info('Value bytes %d.' % len(value))

        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS[command]['struct'] % (len(key), len(value)),
            self.MAGIC['request'],
            self.COMMANDS[command]['command'],
            len(key),
            8, 0, 0, len(key) + len(value) + 8, 0, 0, flags, time, key, value))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            if status == self.STATUS['key_exists']:
                return False
            elif status == self.STATUS['key_not_found']:
                return False
            raise MemcachedException('Code: %d Message: %s' % (status,
                extra_content))

        return True

    def set(self, key, value, time):
        return self._set_add_replace('set', key, value, time)

    def add(self, key, value, time):
        return self._set_add_replace('add', key, value, time)

    def replace(self, key, value, time):
        return self._set_add_replace('replace', key, value, time)

    def _incr_decr(self, command, key, value, default, time):
        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS[command]['struct'] % len(key),
            self.MAGIC['request'],
            self.COMMANDS[command]['command'],
            len(key),
            20, 0, 0, len(key) + 20, 0, 0, value, default, time, key))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d Message: %s' % (status,
                extra_content))

        return struct.unpack('!Q', extra_content)[0]

    def incr(self, key, value, default=0, time=1000000):
        return self._incr_decr('incr', key, value, default,
            time)

    def decr(self, key, value, default=0, time=100):
        return self._incr_decr('decr', key, value, default,
            time)

    def delete(self, key):
        logger.info('Deletting key %s' % key)
        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS['delete']['struct'] % len(key),
            self.MAGIC['request'],
            self.COMMANDS['delete']['command'],
            len(key), 0, 0, 0, len(key), 0, 0, key))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        if status != self.STATUS['success'] \
            and status != self.STATUS['key_not_found']:
            raise MemcachedException('Code: %d message: %s' % (status,
                extra_content))

        logger.debug('Key deleted %s' % key)
        return True

    def flush_all(self, time):
        logger.info('Flushing memcached')
        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS['flush']['struct'],
            self.MAGIC['request'],
            self.COMMANDS['flush']['command'],
            0, 4, 0, 0, 4, 0, 0, time))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d message: %s' % (status,
                extra_content))

        logger.debug('Memcached flushed')
        return True

    def stats(self, key=None):
        if key is not None:
            keylen = len(key)
            packed = struct.pack(
                self.HEADER_STRUCT + '%ds' % keylen,
                self.MAGIC['request'],
                self.COMMANDS['stat']['command'],
                keylen, 0, 0, 0, keylen, 0, 0, key)
        else:
            packed = struct.pack(
                self.HEADER_STRUCT,
                self.MAGIC['request'],
                self.COMMANDS['stat']['command'],
                0, 0, 0, 0, 0, 0, 0)

        self.connection.send(packed)

        value = {}

        while True:
            response = self._get_response()
            keylen = response[2]
            bodylen = response[6]

            if keylen == 0 and bodylen == 0:
                break

            extra_content = response[-1]
            key = extra_content[:keylen]
            body = extra_content[keylen:bodylen]
            value[key] = body

        return value

    def disconnect(self):
        self.connection.close()


class AuthenticationNotSupported(Exception):
    pass


class InvalidCredentials(Exception):
    pass


class MemcachedException(Exception):
    pass
