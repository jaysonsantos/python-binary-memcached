import struct
import socket

HEADER = '!BBHBBHLLQ'
REQUEST = 0x80

AUTH_NEGOTIATION = 0x20
AUTH_REQUEST = 0x21

a = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
a.connect(('localhost', 11211))
a.send(struct.pack(HEADER, REQUEST, AUTH_NEGOTIATION, 0, 0, 0, 0, 0, 0 , 0))

headers = a.recv(24)
(magic, opcode, keylen, extlen, datatype,
    status, bodylen, opaque, cas) = struct.unpack(HEADER, headers)

if status != 0:
    raise Exception('Error playing with the server Error code: %s "%s"' % (status, a.recv(bodylen)))

methods = a.recv(bodylen).split(' ')

if not 'PLAIN' in methods:
    raise Exception('Playing with PLAIN for now')

method = 'PLAIN'

auth = '\x00{username}\x00{password}'.format(username='user', password='password')
a.send(struct.pack(HEADER + '%ds%ds' % (len(method), len(auth)), REQUEST, AUTH_REQUEST, len(method), 0, 0, 0, len(method) + len(auth), 0, 0, method, auth))
headers = a.recv(24)
(magic, opcode, keylen, extlen, datatype,
    status, bodylen, opaque, cas) = struct.unpack(HEADER, headers)
print a.recv(bodylen)
