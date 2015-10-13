from datetime import datetime, timedelta
import logging
import re
import socket
import struct
import threading
try:
    from urllib import splitport
except ImportError:
    from urllib.parse import splitport

try:
    import cPickle as pickle
except ImportError:
    import pickle
    assert pickle

import zlib
from io import BytesIO
import six

from bmemcached.compat import long
from bmemcached.exceptions import AuthenticationNotSupported, InvalidCredentials, MemcachedException
from bmemcached.utils import str_to_bytes


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
        'addq': {'command': 0x12, 'struct': 'LL%ds%ds'},
        'replace': {'command': 0x03, 'struct': 'LL%ds%ds'},
        'delete': {'command': 0x04, 'struct': '%ds'},
        'incr': {'command': 0x05, 'struct': 'QQL%ds'},
        'decr': {'command': 0x06, 'struct': 'QQL%ds'},
        'flush': {'command': 0x08, 'struct': 'I'},
        'noop': {'command': 0x0a, 'struct': ''},
        'stat': {'command': 0x10},
        'auth_negotiation': {'command': 0x20},
        'auth_request': {'command': 0x21, 'struct': '%ds%ds'},
    }

    STATUS = {
        'success': 0x00,
        'key_not_found': 0x01,
        'key_exists': 0x02,
        'auth_error': 0x08,
        'unknown_command': 0x81,

        # This is used internally, and is never returned by the server.  (The server returns a 16-bit
        # value, so it's not capable of returning this value.)
        'server_disconnected': 0xFFFFFFFF,
    }

    FLAGS = {
        'pickle': 1 << 0,
        'integer': 1 << 1,
        'long': 1 << 2,
        'compressed': 1 << 3
    }

    COMPRESSION_THRESHOLD = 128

    def __init__(self, server, username=None, password=None, compression=None, socket_timeout=None, pickle_protocol=0,
                 pickler=None, unpickler=None):
        super(Protocol, self).__init__()
        self.server = server
        self._username = username
        self._password = password

        self.compression = zlib if compression is None else compression
        self.connection = None
        self.authenticated = False
        self.socket_timeout = socket_timeout
        self.pickle_rotocol = pickle_protocol
        self.pickler = pickler
        self.unpickler = unpickler

        self.reconnects_deferred_until = None

        if not server.startswith('/'):
            self.host, self.port = self.split_host_port(self.server)
            self.set_retry_delay(5)
        else:
            self.host = self.port = None
            self.set_retry_delay(0)

    @property
    def server_uses_unix_socket(self):
        return self.host is None

    def set_retry_delay(self, value):
        self.retry_delay = value

    def _open_connection(self):
        if self.connection:
            return

        self.authenticated = False

        # If we're deferring a reconnection attempt, wait.
        if self.reconnects_deferred_until and self.reconnects_deferred_until > datetime.now():
            return

        try:
            if self.host:
                self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.connection.settimeout(self.socket_timeout)
                self.connection.connect((self.host, self.port))
            else:
                self.connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.connection.connect(self.server)

            self._send_authentication()
        except socket.error:
            # If the connection attempt fails, start delaying retries.
            self.reconnects_deferred_until = datetime.now() + timedelta(seconds=self.retry_delay)
            raise

    def _connection_error(self, exception):
        # On error, clear our dead connection.
        self.disconnect()

    @classmethod
    def split_host_port(cls, server):
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
        :rtype: six.string_type
        """
        value = b''
        while len(value) < size:
            data = self.connection.recv(size - len(value))
            if not data:
                break
            value += data

        # If we got less data than we requested, the server disconnected.
        if len(value) < size:
            raise socket.error()

        return value

    def _get_response(self):
        """
        Get memcached response from socket.

        :return: A tuple with binary values from memcached.
        :rtype: tuple
        """
        try:
            self._open_connection()
            if self.connection is None:
                # The connection wasn't opened, which means we're deferring a reconnection attempt.
                # Raise a socket.error, so we'll return the same server_disconnected message as we
                # do below.
                raise socket.error('Delaying reconnection attempt')

            header = self._read_socket(self.HEADER_SIZE)
            (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
             cas) = struct.unpack(self.HEADER_STRUCT, header)

            assert magic == self.MAGIC['response']

            extra_content = None
            if bodylen:
                extra_content = self._read_socket(bodylen)

            return (magic, opcode, keylen, extlen, datatype, status, bodylen,
                    opaque, cas, extra_content)
        except socket.error as e:
            self._connection_error(e)

            # (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas, extra_content)
            message = str(e)
            return (self.MAGIC['response'], -1, 0, 0, 0, self.STATUS['server_disconnected'], 0, 0, 0, message)

    def _send(self, data):
        try:
            self._open_connection()
            if self.connection is None:
                return

            self.connection.sendall(data)
        except socket.error as e:
            self._connection_error(e)

    def authenticate(self, username, password):
        """
        Authenticate user on server.

        :param username: Username used to be authenticated.
        :type username: six.string_type
        :param password: Password used to be authenticated.
        :type password: six.string_type
        :return: True if successful.
        :raises: InvalidCredentials, AuthenticationNotSupported, MemcachedException
        :rtype: bool
        """
        self._username = username
        self._password = password

        # Reopen the connection with the new credentials.
        self.disconnect()
        self._open_connection()
        return self.authenticated

    def _send_authentication(self):
        if not self._username or not self._password:
            return False

        logger.info('Authenticating as %s' % self._username)
        self._send(struct.pack(self.HEADER_STRUCT,
                               self.MAGIC['request'],
                               self.COMMANDS['auth_negotiation']['command'],
                               0, 0, 0, 0, 0, 0, 0))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status == self.STATUS['server_disconnected']:
            return False

        if status == self.STATUS['unknown_command']:
            logger.debug('Server does not requires authentication.')
            self.authenticated = True
            return True

        methods = extra_content

        if 'PLAIN' not in methods:
            raise AuthenticationNotSupported('This module only supports '
                                             'PLAIN auth for now.')

        method = b'PLAIN'
        auth = '\x00%s\x00%s' % (self._username, self._password)
        if six.PY3:
            auth = auth.encode()

        self._send(struct.pack(self.HEADER_STRUCT +
                               self.COMMANDS['auth_request']['struct'] % (len(method), len(auth)),
                               self.MAGIC['request'], self.COMMANDS['auth_request']['command'],
                               len(method), 0, 0, 0, len(method) + len(auth), 0, 0, method, auth))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status == self.STATUS['server_disconnected']:
            return False

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
        :type value: six.string_type, int, long, object
        :return: Serialized type
        :rtype: str
        """
        flags = 0
        if isinstance(value, str):
            if six.PY3:
                value = value.encode('utf8')
        elif isinstance(value, int) and isinstance(value, bool) is False:
            flags |= self.FLAGS['integer']
            value = str(value)
        elif isinstance(value, long) and isinstance(value, bool) is False:
            flags |= self.FLAGS['long']
            value = str(value)
        else:
            flags |= self.FLAGS['pickle']
            buf = BytesIO()
            pickler = self.pickler(buf, self.pickle_rotocol)
            pickler.dump(value)
            value = buf.getvalue()

        if len(value) > self.COMPRESSION_THRESHOLD:
            value = self.compression.compress(value)
            flags |= self.FLAGS['compressed']

        return flags, value

    def deserialize(self, value, flags):
        """
        Deserialized values based on flags or just return it if it is not serialized.

        :param value: Serialized or not value.
        :type value: six.string_type, int
        :param flags: Value flags
        :type flags: int
        :return: Deserialized value
        :rtype: six.string_type|int
        """
        to_str = lambda v: v.decode('utf8') if six.PY3 else v

        if flags & self.FLAGS['compressed']:  # pragma: no branch
            value = self.compression.decompress(value)

        if flags & self.FLAGS['integer']:
            return int(to_str(value))
        elif flags & self.FLAGS['long']:
            return long(to_str(value))
        elif flags & self.FLAGS['pickle']:
            buf = BytesIO(value)

            unpickler = self.unpickler(buf)
            return unpickler.load()

        return to_str(value)

    def get(self, key):
        """
        Get a key and its CAS value from server.  If the value isn't cached, return
        (None, None).

        :param key: Key's name
        :type key: six.string_type
        :return: Returns (value, cas).
        :rtype: object
        """
        logger.info('Getting key %s' % key)
        data = struct.pack(self.HEADER_STRUCT +
                           self.COMMANDS['get']['struct'] % (len(key)),
                           self.MAGIC['request'],
                           self.COMMANDS['get']['command'],
                           len(key), 0, 0, 0, len(key), 0, 0, str_to_bytes(key))
        self._send(data)

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        logger.debug('Value Length: %d. Body length: %d. Data type: %d' % (
            extlen, bodylen, datatype))

        if status != self.STATUS['success']:
            if status == self.STATUS['key_not_found']:
                logger.debug('Key not found. Message: %s'
                             % extra_content)
                return None, None

            if status == self.STATUS['server_disconnected']:
                return None, None

            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        flags, value = struct.unpack('!L%ds' % (bodylen - 4, ), extra_content)

        return self.deserialize(value, flags), cas

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
        if six.PY2:
            msg = ''
        else:
            msg = b''
        msg = msg.join([
            struct.pack(self.HEADER_STRUCT +
                        self.COMMANDS['getkq']['struct'] % (len(key)),
                        self.MAGIC['request'],
                        self.COMMANDS['getkq']['command'],
                        len(key), 0, 0, 0, len(key), 0, 0, str_to_bytes(key))
            for key in keys])
        msg += struct.pack(self.HEADER_STRUCT +
                           self.COMMANDS['getk']['struct'] % (len(last)),
                           self.MAGIC['request'],
                           self.COMMANDS['getk']['command'],
                           len(last), 0, 0, 0, len(last), 0, 0, last.encode())

        self._send(msg)

        d = {}
        opcode = -1
        while opcode != self.COMMANDS['getk']['command']:
            (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
             cas, extra_content) = self._get_response()

            if status == self.STATUS['success']:
                flags, key, value = struct.unpack('!L%ds%ds' %
                                                  (keylen, bodylen - keylen - 4),
                                                  extra_content)
                d[key.decode()] = self.deserialize(value, flags), cas
            elif status == self.STATUS['server_disconnected']:
                break
            elif status != self.STATUS['key_not_found']:
                raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        return d

    def _set_add_replace(self, command, key, value, time, cas=0):
        """
        Function to set/add/replace commands.

        :param key: Key's name
        :type key: six.string_type
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :param cas: The CAS value that must be matched for this operation to complete, or 0 for no CAS.
        :type cas: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        logger.info('Setting/adding/replacing key %s.' % key)
        flags, value = self.serialize(value)
        logger.info('Value bytes %d.' % len(value))
        if six.PY3 and isinstance(value, str):
            value = value.encode('utf8')

        self._send(struct.pack(self.HEADER_STRUCT +
                               self.COMMANDS[command]['struct'] % (len(key), len(value)),
                               self.MAGIC['request'],
                               self.COMMANDS[command]['command'],
                               len(key), 8, 0, 0, len(key) + len(value) + 8, 0, cas, flags,
                               time, str_to_bytes(key), value))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status != self.STATUS['success']:
            if status == self.STATUS['key_exists']:
                return False
            elif status == self.STATUS['key_not_found']:
                return False
            elif status == self.STATUS['server_disconnected']:
                return False
            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))

        return True

    def set(self, key, value, time):
        """
        Set a value for a key on server.

        :param key: Key's name
        :type key: six.string_type
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        return self._set_add_replace('set', key, value, time)

    def cas(self, key, value, cas, time):
        """
        Add a key/value to server ony if it does not exist.

        :param key: Key's name
        :type key: six.string_type
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True if key is added False if key already exists and has a different CAS
        :rtype: bool
        """
        # The protocol CAS value 0 means "no cas".  Calling cas() with that value is
        # probably unintentional.  Don't allow it, since it would overwrite the value
        # without performing CAS at all.
        assert cas != 0, '0 is an invalid CAS value'

        # If we get a cas of None, interpret that as "compare against nonexistant and set",
        # which is simply Add.
        if cas is None:
            return self._set_add_replace('add', key, value, time)
        else:
            return self._set_add_replace('set', key, value, time, cas=cas)

    def add(self, key, value, time):
        """
        Add a key/value to server ony if it does not exist.

        :param key: Key's name
        :type key: six.string_type
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
        :type key: six.string_type
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
        Set multiple keys with its values on server.

        If a key is a (key, cas) tuple, insert as if cas(key, value, cas) had
        been called.

        :param mappings: A dict with keys/values
        :type mappings: dict
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: True
        :rtype: bool
        """
        mappings = mappings.items()
        msg = []

        for key, value in mappings:
            if isinstance(key, tuple):
                key, cas = key
            else:
                cas = None

            if cas == 0:
                # Like cas(), if the cas value is 0, treat it as compare-and-set against not
                # existing.
                command = 'addq'
            else:
                command = 'setq'

            flags, value = self.serialize(value)
            m = struct.pack(self.HEADER_STRUCT +
                            self.COMMANDS[command]['struct'] % (len(key), len(value)),
                            self.MAGIC['request'],
                            self.COMMANDS[command]['command'],
                            len(key),
                            8, 0, 0, len(key) + len(value) + 8, 0, cas or 0,
                            flags, time, str_to_bytes(key), value)
            msg.append(m)

        m = struct.pack(self.HEADER_STRUCT +
                        self.COMMANDS['noop']['struct'],
                        self.MAGIC['request'],
                        self.COMMANDS['noop']['command'],
                        0, 0, 0, 0, 0, 0, 0)
        msg.append(m)

        if six.PY2:
            msg = ''.join(msg)
        else:
            msg = b''.join(msg)

        self._send(msg)

        opcode = -1
        retval = True
        while opcode != self.COMMANDS['noop']['command']:
            (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
             cas, extra_content) = self._get_response()
            if status != self.STATUS['success']:
                retval = False
            if status == self.STATUS['server_disconnected']:
                break

        return retval

    def _incr_decr(self, command, key, value, default, time):
        """
        Function which increments and decrements.

        :param key: Key's name
        :type key: six.string_type
        :param value: Number to be (de|in)cremented
        :type value: int
        :param default: Default value if key does not exist.
        :type default: int
        :param time: Time in seconds to expire key.
        :type time: int
        :return: Actual value of the key on server
        :rtype: int
        """
        self._send(struct.pack(self.HEADER_STRUCT +
                               self.COMMANDS[command]['struct'] % len(key),
                               self.MAGIC['request'],
                               self.COMMANDS[command]['command'],
                               len(key),
                               20, 0, 0, len(key) + 20, 0, 0, value,
                               default, time, str_to_bytes(key)))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status not in (self.STATUS['success'], self.STATUS['server_disconnected']):
            raise MemcachedException('Code: %d Message: %s' % (status, extra_content))
        if status == self.STATUS['server_disconnected']:
            return 0

        return struct.unpack('!Q', extra_content)[0]

    def incr(self, key, value, default=0, time=1000000):
        """
        Increment a key, if it exists, returns it's actual value, if it don't, return 0.

        :param key: Key's name
        :type key: six.string_type
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
        :type key: six.string_type
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

    def delete(self, key, cas=0):
        """
        Delete a key/value from server. If key existed and was deleted, it returns True.

        :param key: Key's name to be deleted
        :type key: six.string_type
        :param cas: If set, only delete the key if its CAS value matches.
        :type cas: int
        :return: True in case o success and False in case of failure.
        :rtype: bool
        """
        logger.info('Deleting key %s' % key)
        self._send(struct.pack(self.HEADER_STRUCT +
                               self.COMMANDS['delete']['struct'] % len(key),
                               self.MAGIC['request'],
                               self.COMMANDS['delete']['command'],
                               len(key), 0, 0, 0, len(key), 0, cas, str_to_bytes(key)))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status == self.STATUS['server_disconnected']:
            return False
        if status != self.STATUS['success'] and status not in (self.STATUS['key_not_found'], self.STATUS['key_exists']):
            raise MemcachedException('Code: %d message: %s' % (status, extra_content))

        logger.debug('Key deleted %s' % key)
        return status != self.STATUS['key_exists']

    def delete_multi(self, keys):
        """
        Delete multiple keys from server in one command.

        :param keys: A list of keys to be deleted
        :type keys: list
        :return: True in case of success and False in case of failure.
        :rtype: bool
        """
        logger.info('Deleting keys %r' % keys)
        if six.PY2:
            msg = ''
        else:
            msg = b''
        for key in keys:
            msg += struct.pack(
                self.HEADER_STRUCT +
                self.COMMANDS['delete']['struct'] % len(key),
                self.MAGIC['request'],
                self.COMMANDS['delete']['command'],
                len(key), 0, 0, 0, len(key), 0, 0, str_to_bytes(key))

        msg += struct.pack(
            self.HEADER_STRUCT +
            self.COMMANDS['noop']['struct'],
            self.MAGIC['request'],
            self.COMMANDS['noop']['command'],
            0, 0, 0, 0, 0, 0, 0)

        self._send(msg)

        opcode = -1
        retval = True
        while opcode != self.COMMANDS['noop']['command']:
            (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
             cas, extra_content) = self._get_response()
            if status != self.STATUS['success']:
                retval = False
            if status == self.STATUS['server_disconnected']:
                break

        return retval

    def flush_all(self, time):
        """
        Send a command to server flush|delete all keys.

        :param time: Time to wait until flush in seconds.
        :type time: int
        :return: True in case of success, False in case of failure
        :rtype: bool
        """
        logger.info('Flushing memcached')
        self._send(struct.pack(self.HEADER_STRUCT +
                               self.COMMANDS['flush']['struct'],
                               self.MAGIC['request'],
                               self.COMMANDS['flush']['command'],
                               0, 4, 0, 0, 4, 0, 0, time))

        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
         cas, extra_content) = self._get_response()

        if status not in (self.STATUS['success'], self.STATUS['server_disconnected']):
            raise MemcachedException('Code: %d message: %s' % (status, extra_content))

        logger.debug('Memcached flushed')
        return True

    def stats(self, key=None):
        """
        Return server stats.

        :param key: Optional if you want status from a key.
        :type key: six.string_type
        :return: A dict with server stats
        :rtype: dict
        """
        # TODO: Stats with key is not working.
        if key is not None:
            if isinstance(key, str) and six.PY3:
                key = str_to_bytes(key)
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

        self._send(packed)

        value = {}

        while True:
            response = self._get_response()

            status = response[5]
            if status == self.STATUS['server_disconnected']:
                break

            keylen = response[2]
            bodylen = response[6]

            if keylen == 0 and bodylen == 0:
                break

            extra_content = response[-1]
            key = extra_content[:keylen]
            body = extra_content[keylen:bodylen]
            value[key.decode() if isinstance(key, bytes) else key] = body

        return value

    def disconnect(self):
        """
        Disconnects from server.  A new connection will be established the next time a request is made.

        :return: Nothing
        :rtype: None
        """
        if self.connection:
            self.connection.close()
            self.connection = None
