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

    COMMANDS = {
        'get': {'command': 0x00, 'struct': '%ds'},
        'set': {'command': 0x01},
        'auth_negotiation': {'command': 0x20},
        'auth_request': {'command': 0x21, 'struct': '%ds%ds'}
    }

    def __init__(self, server):
        pass
