import struct
import socket
import logging

__all__ = ['Client']
logger = logging.getLogger('bmemcached')


class Client(object):
    def __init__(self, servers):
        self.set_servers(servers)

    def set_servers(self, servers):
        self.servers = [Server(server) for server in servers]


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
        'set': {'command': 0x01},
        'auth_negotiation': {'command': 0x20},
        'auth_request': {'command': 0x21, 'struct': '%ds%ds'}
    }

    STATUS = {
        'success': 0x00,
        'unknown_command': 0x81
    }

    def __init__(self, server, username=None, password=None):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server = server.split(':')
        host = server[0]
        if len(server) > 1:
            try:
                port = int(server[1])
            except (ValueError, TypeError):
                port = 11211
        else:
            port = 11211

        self.connection.connect((host, port))
        if username and password:
            self.authenticate(username, password)

    def authenticate(self, username, password):
        logger.info('Authenticating as %s' % username)
        self.connection.send(struct.pack(self.HEADER_STRUCT,
            self.MAGIC['request'],
            self.COMMANDS['auth_negotiation']['command'],
            0, 0, 0, 0, 0, 0, 0))
        header = self.connection.recv(self.HEADER_SIZE)
        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas) = struct.unpack(self.HEADER_STRUCT, header)

        if status == self.STATUS['unknown_command']:
            logger.debug('Server does not requires authentication.')
            return True

        methods = self.connection.recv(bodylen).split(' ')

        if not 'PLAIN' in methods:
            raise AuthenticationNotSupported('This module only supports ' + \
                'PLAIN auth for now.')

        method = 'PLAIN'
        auth = '\x00%s\x00%s' % (username, password)
        self.connection.send(struct.pack(self.HEADER_STRUCT + \
            self.COMMANDS['auth_request']['struct'] % (len(method), len(auth)),
            self.MAGIC['request'], self.COMMANDS['auth_request']['command'],
            len(method), 0, 0, 0, len(method) + len(auth), 0, 0, method, auth))
        headers = self.connection.recv(self.HEADER_SIZE)
        (magic, opcode, keylen, extlen, datatype, status, bodylen,
            opaque, cas) = struct.unpack(self.HEADER_STRUCT, headers)

        if status != self.STATUS['success']:
            raise MemcachedException('Code: %d Message: %s' % (status,
                self.connection.recv(bodylen)))

        logger.debug('Auth OK. Code: %d Message: %s' % (status,
            self.connection.recv(bodylen)))

        return True


class AuthenticationNotSupported(Exception):
    pass


class MemcachedException(Exception):
    pass
