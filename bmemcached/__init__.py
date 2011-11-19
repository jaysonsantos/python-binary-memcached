import struct
import socket

__all__ = ['Client']


class Client(object):
    def __init__(self, servers):
        self.set_servers(servers)

    def set_servers(self, servers):
        self.servers = [Server(server) for server in servers]


class Server(object):
    HEADER_STRUCT = '!BBHBBHLLQ'

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

    def __init__(self, server, username=None, password=None):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server = server.split(':')
        host = server[0]
        if len(server) >1:
            try:
                port = int(server[1])
            except (ValuError, TypeError):
                port = 11211
        else:
            port = 11211
        self.connection.connect((host, port))

