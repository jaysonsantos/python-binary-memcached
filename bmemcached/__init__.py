import struct
import socket

__all__ = ['Client']


class Client(object):
    def __init__(self, servers):
        self.set_servers(servers)
    
    def set_servers(self, servers):
        self.servers = [Server(server) for server in servers]
    

class Server(object):
    def __init__(self, server):
        pass
