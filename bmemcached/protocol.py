from cPickle import dumps, loads
import logging
import re
import socket
import struct
import threading
from urllib import splitport
import zlib

from bmemcached.exceptions import AuthenticationNotSupported, InvalidCredentials, MemcachedException


logger = logging.getLogger(__name__)


class Protocol(threading.local):
    """
    This class is used by Client class to communicate with server.
    """
    HEADER_STRUCT = '!BBHBBHLLQ'
    HEADER_SIZE = 24

    MAGIC = {
        'request': 0x80,
        'response': 0x81
    }

    # All structures will be appended to HEADER_STRUCT
    COMMANDS = {
        'get': {'command': 0x00, 'struct': '%ds'},
        'getk': {'command': 0x0C, 'struct': '%ds'},
        'getkq': {'command': 0x0D, 'struct': '%ds'},
        'set': {'command': 0x01, 'struct': 'LL%ds%ds'},
        'setq': {'command': 0x11, 'struct': 'LL%ds%ds'},
        'add': {'command': 0x02, 'struct': 'LL%ds%ds'},
        'replace': {'command': 0x03, 'struct': 'LL%ds%ds'},
        'delete': {'command': 0x04, 'struct': '%ds'},
        'incr': {'command': 0x05, 'struct': 'QQL%ds'},
        'decr': {'command': 0x06, 'struct': 'QQL%ds'},
        'flush': {'command': 0x08, 'struct': 'I'},
        'stat': {'command': 0x10},
        'auth_negotiation': {'command': 0x20},
        'auth_request': {'command': 0x21, 'struct': '%ds%ds'},
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

    COMPRESSION_THRESHOLD = 128

    def __init__(self, server, username=None, password=None, compression=None):
        self.server = server
        self.authenticated = False
        self.compression = zlib if compression is None else compression

        if server.startswith('/'):
            self.connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.connection.connect(server)
        else:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
        return host, port

    def _read_socket(self, size):
        """
        Reads data from socket.

        :param size: Size in bytes to be read.
        :type size: int
        :return: Data from socket
        :rtype: basestring
        """
        value = ''
        while len(value) < size:
            data = self.connection.recv(size - len(value))
            if not data:
                break
            value += data
        assert len(value) == size, "Asked for %d bytes, got %d" % (size, len(value))
        return value

    def _get_response(self):
        """
        Get memcached response from socket.

        :return: A tuple with binary values from memcached.
        :rtype: tuple
        """
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
        """
        Authenticate user on server.

        :param username: Username used to be authenticated.
        :type username: basestring
        :param password: Password used to be authenticated.
        :type password: basestring
        :return: True if successful.
        :raises: InvalidCredentials, AuthenticationNotSupported, MemcachedException
        :rtype: bool
        """
        logger.info('Authenticating as %s' % username)
        self.connection.sendall(struct.pack(self.HEADER_STRUCT,
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
            raise AuthenticationNotSupported('This module only supports '
                                             'PLAIN auth for now.')

        method = 'PLAIN'
        auth = '\x00%s\x00%s' % (username, password)
        self.connection.sendall(struct.pack(self.HEADER_STRUCT +
                                         self.COMMANDS['auth_request']['struct'] % (len(method), len(auth)),
                                         self.MAGIC['request'], self.COMMANDS['auth_request']['command'],
                                         len(method), 0, 0, 0, len(method) + len(auth), 0, 0, method, auth))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status == self.STATUS['auth_error']:
            raise InvalidCredentials("Incorrect username or password")

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        logger.debug('Auth OK. Code: %d Message: %s' % (status, extra_content))

        self.authenticated = True
        return True

    def serialize(self, value):
        """
        Serializes a value based on it's type.

        :param value: Something to be serialized
        :type value: basestring, int, long, object
        :return: Serialized type
        :rtype: str
        """
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

        if len(value) > self.COMPRESSION_THRESHOLD:
            value = self.compression.compress(value)
            flags |= self.FLAGS['compressed']

        return flags, value

    def deserialize(self, value, flags):
        """
        Deserialized values based on flags or just return it if it is not serialized.

        :param value: Serialized or not value.
        :type value: basestring, int
        :param flags: Value flags
        :type flags: int
        :return: Deserialized value
        :rtype: basestring|int
        """
        if flags & self.FLAGS['compressed']:  # pragma: no branch
            value = self.compression.decompress(value)

        if flags & self.FLAGS['integer']:
            return int(value)
        elif flags & self.FLAGS['long']:
            return long(value)
        elif flags & self.FLAGS['pickle']:
            return loads(value)

        return value

    def get(self, key):
        """
        Get a key from server.

        :param key: Key's name
        :type key: basestring
        :return: Returns a key data from server.
        :rtype: object
        """
        logger.info('Getting key %s' % key)
        self.connection.sendall(struct.pack(self.HEADER_STRUCT +
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
                logger.debug('Key not found. Message: %s'
                             % extra_content)
                return None

            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        flags, value = struct.unpack('!L%ds' % (bodylen - 4, ), extra_content)

        return self.deserialize(value, flags)

    def get_multi(self, keys):
        """
        Get multiple keys from server.

        :param keys: A list of keys to from server.
        :type keys: list
        :return: A dict with all requested keys.
        :rtype: dict
        """
        # pipeline N-1 getkq requests, followed by a regular getk to uncork the
        # server
        keys, last = keys[:-1], keys[-1]
        msg = ''.join([
            struct.pack(self.HEADER_STRUCT +
                        self.COMMANDS['getkq']['struct'] % (len(key)),
                        self.MAGIC['request'],
                        self.COMMANDS['getkq']['command'],
                        len(key), 0, 0, 0, len(key), 0, 0, key)
            for key in keys])
        msg += struct.pack(self.HEADER_STRUCT +
                           self.COMMANDS['getk']['struct'] % (len(last)),
                           self.MAGIC['request'],
                           self.COMMANDS['getk']['command'],
                           len(last), 0, 0, 0, len(last), 0, 0, last)

        self.connection.sendall(msg)

        d = {}
        opcode = -1
        while opcode != self.COMMANDS['getk']['command']:
            (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
             cas, extra_content) = self._get_response()

            if status == self.STATUS['success']:
                flags, key, value = struct.unpack('!L%ds%ds' %
                                                  (keylen, bodylen - keylen - 4),
                                                  extra_content)
                d[key] = self.deserialize(value, flags)
            elif status != self.STATUS['key_not_found']:
                raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        return d

    def _set_add_replace(self, command, key, value, time):
        """
        Function to set/add/replace commands.

        :param key: Key's name
        :type key: basestring
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        logger.info('Setting/adding/replacing key %s.' % key)
        flags, value = self.serialize(value)
        logger.info('Value bytes %d.' % len(value))

        self.connection.sendall(struct.pack(self.HEADER_STRUCT +
                                         self.COMMANDS[command]['struct'] % (len(key), len(value)),
                                         self.MAGIC['request'],
                                         self.COMMANDS[command]['command'],
                                         len(key),
                                         8, 0, 0, len(key) + len(value) + 8, 0, 0, flags,
                                         time, key, value))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            if status == self.STATUS['key_exists']:
                return False
            elif status == self.STATUS['key_not_found']:
                return False
            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        return True

    def set(self, key, value, time):
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
        return self._set_add_replace('set', key, value, time)

    def add(self, key, value, time):
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
        return self._set_add_replace('add', key, value, time)

    def replace(self, key, value, time):
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
        return self._set_add_replace('replace', key, value, time)

    def set_multi(self, mappings, time=100):
        """
        Set multiple keys with it's values on server.

        :param mappings: A dict with keys/values
        :type mappings: dict
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        mappings = mappings.items()
        mappings, last = mappings[:-1], mappings[-1]
        msg = []
        for key, value in mappings:
            flags, value = self.serialize(value)
            m = struct.pack(self.HEADER_STRUCT +
                            self.COMMANDS['setq']['struct'] % (len(key), len(value)),
                            self.MAGIC['request'],
                            self.COMMANDS['setq']['command'],
                            len(key),
                            8, 0, 0, len(key) + len(value) + 8, 0, 0,
                            flags, time, key, value)
            msg.append(m)

        key, value = last
        flags, value = self.serialize(value)
        msg.append(struct.pack(self.HEADER_STRUCT +
                               self.COMMANDS['set']['struct'] % (len(key), len(value)),
                               self.MAGIC['request'],
                               self.COMMANDS['set']['command'],
                               len(key),
                               8, 0, 0, len(key) + len(value) + 8, 0, 0,
                               flags, time, key, value))

        msg = ''.join(msg)

        self.connection.sendall(msg)

        opcode = -1
        retval = True
        while opcode != self.COMMANDS['set']['command']:
            (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
             cas, extra_content) = self._get_response()
            if status != self.STATUS['success']:
                retval = False

        return retval

    def _incr_decr(self, command, key, value, default, time):
        """
        Function which increments and decrements.

        :param key: Key's name
        :type key: basestring
        :param value: Number to be (de|in)cremented
        :type value: int
        :param default: Default value if key does not exist.
        :type default: int
        :param time: Time in seconds to expire key.
        :type time: int
        :return: Actual value of the key on server
        :rtype: int
        """
        self.connection.sendall(struct.pack(self.HEADER_STRUCT +
                                         self.COMMANDS[command]['struct'] % len(key),
                                         self.MAGIC['request'],
                                         self.COMMANDS[command]['command'],
                                         len(key),
                                         20, 0, 0, len(key) + 20, 0, 0, value,
                                         default, time, key))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        return struct.unpack('!Q', extra_content)[0]

    def incr(self, key, value, default=0, time=1000000):
        """
        Increment a key, if it exists, returns it's actual value, if it don't, return 0.

        :param key: Key's name
        :type key: basestring
        :param value: Number to be incremented
        :type value: int
        :param default: Default value if key does not exist.
        :type default: int
        :param time: Time in seconds to expire key.
        :type time: int
        :return: Actual value of the key on server
        :rtype: int
        """
        return self._incr_decr('incr', key, value, default, time)

    def decr(self, key, value, default=0, time=100):
        """
        Decrement a key, if it exists, returns it's actual value, if it don't, return 0.
        Minimum value of decrement return is 0.

        :param key: Key's name
        :type key: basestring
        :param value: Number to be decremented
        :type value: int
        :param default: Default value if key does not exist.
        :type default: int
        :param time: Time in seconds to expire key.
        :type time: int
        :return: Actual value of the key on server
        :rtype: int
        """
        return self._incr_decr('decr', key, value, default, time)

    def delete(self, key):
        """
        Delete a key/value from server. If key does not exist, it returns True.

        :param key: Key's name to be deleted
        :type key: basestring
        :return: True in case o success and False in case of failure.
        :rtype: bool
        """
        logger.info('Deletting key %s' % key)
        self.connection.sendall(struct.pack(self.HEADER_STRUCT +
                                         self.COMMANDS['delete']['struct'] % len(key),
                                         self.MAGIC['request'],
                                         self.COMMANDS['delete']['command'],
                                         len(key), 0, 0, 0, len(key), 0, 0, key))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status != self.STATUS['success'] and status != self.STATUS['key_not_found']:
            raise MemcachedException('Code: %d message: %s' % (status, extra_content))

        logger.debug('Key deleted %s' % key)
        return True

    def flush_all(self, time):
        """
        Send a command to server flush|delete all keys.

        :param time: Time to wait until flush in seconds.
        :type time: int
        :return: True in case of success, False in case of failure
        :rtype: bool
        """
        logger.info('Flushing memcached')
        self.connection.sendall(struct.pack(self.HEADER_STRUCT +
                                         self.COMMANDS['flush']['struct'],
                                         self.MAGIC['request'],
                                         self.COMMANDS['flush']['command'],
                                         0, 4, 0, 0, 4, 0, 0, time))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d message: %s' % (status, extra_content))

        logger.debug('Memcached flushed')
        return True

    def stats(self, key=None):
        """
        Return server stats.

        :param key: Optional if you want status from a key.
        :type key: basestring
        :return: A dict with server stats
        :rtype: dict
        """
        # TODO: Stats with key is not working.
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

        self.connection.sendall(packed)

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
        """
        Disconnects from server.

        :return: Nothing
        :rtype: None
        """
        self.connection.close()
