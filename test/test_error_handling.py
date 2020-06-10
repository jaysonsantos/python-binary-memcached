import multiprocessing
import os
import select
import six
import socket
import time
import unittest

import bmemcached
from bmemcached.protocol import Protocol


class _CacheProxy(multiprocessing.Process):
    def __init__(self, server, pipe, listen_port=None):
        super(_CacheProxy, self).__init__()
        self._listen_port = listen_port
        self.server = server
        self.pipe = pipe

    def run(self):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.setblocking(False)
        listen_sock.bind((os.environ['MEMCACHED_HOST'], self._listen_port or 0))
        listen_sock.listen(1)

        # Tell our caller the (host, port) that we're listening on.
        self.pipe.send(listen_sock.getsockname())

        # Open a connection to the real memcache server.
        if not self.server.startswith('/'):
            host, port = Protocol.split_host_port(self.server)
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.connect((host, port))
        else:
            server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_sock.connect(self.server)

        # The connection to this server above is blocking, but reads and writes below are nonblocking.
        server_sock.setblocking(False)

        # listen_sock is the socket we're listening for connections on.  We only handle
        # a single connection at a time.
        # client_sock is the connection we've accepted from listen_sock.
        # server_sock is the connection to the actual server.
        client_sock = None

        # Data waiting to be sent to client_sock:
        data_for_client = b''

        # Data waiting to be sent to server_sock:
        data_for_server = b''

        while True:
            read_sockets = [listen_sock]
            write_sockets = []

            if client_sock:
                # Only add client_sock to read_sockets if we don't already have data
                # from it waiting to be sent to the real server.
                if not data_for_server:
                    read_sockets.append(client_sock)

                # Only add client_sock to write_sockets if we have data to send.
                if data_for_client:
                    write_sockets.append(client_sock)

            if not data_for_client:
                read_sockets.append(server_sock)
            if data_for_server:
                write_sockets.append(server_sock)

            r, w, _ = select.select(read_sockets, write_sockets, [])
            if listen_sock in r:
                if client_sock:
                    client_sock.close()
                client_sock, client_addr = listen_sock.accept()
                client_sock.setblocking(False)

            if server_sock in r:
                data_for_client += server_sock.recv(1024)

            if client_sock in r:
                data_for_server += client_sock.recv(1024)

            if server_sock in w:
                bytes_written = server_sock.send(data_for_server)
                data_for_server = data_for_server[bytes_written:]

            if client_sock in w:
                bytes_written = client_sock.send(data_for_client)
                data_for_client = data_for_client[bytes_written:]


class MemcachedTests(unittest.TestCase):
    def setUp(self):
        self._proxy_port = None

        # Start a helper to proxy requests to the actual memcache server.  This uses a
        # process instead of a thread, so we can simply kill the process between tests.
        self._start_proxy()
        self._stop_proxy()
        self._start_proxy()

        self.client = bmemcached.Client(self.server, 'user', 'password')

        # Disable retry delays, so we can disconnect and reconnect from the
        # server without needing to put delays in most of the tests.
        self.client.enable_retry_delay(False)

        # Clean up from any previous tests.
        self.client.delete('test_key')
        self.client.delete('test_key2')

    def _server_host(self):
        return '{}:11211'.format(os.environ['MEMCACHED_HOST'])

    def _start_proxy(self):
        # Start the proxy.  If this isn't the first time we've started the proxy,
        # use the same port we got the first time around.
        parent_pipe, child_pipe = multiprocessing.Pipe()
        self._proxy_thread = _CacheProxy(self._server_host(), child_pipe, self._proxy_port)
        self._proxy_thread.start()

        # Read the port the server is actually listening on.  If we supplied a port, it
        # will always be the same.  This also guarantees that the process is listening on
        # the port before we continue and try to connect to it.
        sockname = parent_pipe.recv()
        self._proxy_port = sockname[1]
        self.server = '%s:%i' % sockname

    def _stop_proxy(self):
        if not self._proxy_thread:
            return

        # Kill the proxy, which causes communication to the server to fail.
        self._proxy_thread.terminate()
        self._proxy_thread.join()
        self._proxy_thread = None

    def tearDown(self):
        self.client.disconnect_all()
        self._stop_proxy()

    def testSet(self):
        self.assertTrue(self.client.set('test_key', 'test'))
        self._stop_proxy()
        self.assertFalse(self.client.set('test_key', 'test'))

    def testSetMulti(self):
        six.assertCountEqual(self, self.client.set_multi({
            'test_key': 'value',
            'test_key2': 'value2'}), [])

        self._stop_proxy()

        six.assertCountEqual(self, self.client.set_multi({
            'test_key': 'value',
            'test_key2': 'value2'}), ['test_key', 'test_key2'])

    def testGet(self):
        self.client.set('test_key', 'test')
        self.assertEqual('test', self.client.get('test_key'))

        # If the server is offline, get always returns None.
        self._stop_proxy()
        self.assertTrue(self.client.get('test_key') is None)

        # After the server comes back online, gets will resume.
        self._start_proxy()
        self.assertEqual('test', self.client.get('test_key'))

    def testRetryDelay(self):
        # Test delaying retries.  We only enable retry delays for this test, since we
        # need to pause to test it, which slows down the test.
        self.client._set_retry_delay(0.25)

        self.client.set('test_key', 'test')
        self.assertEqual('test', self.client.get('test_key'))

        # If the server is offline, get always returns None.  This request will cause
        # the client to notice that the connection is offline, but not to retry the
        # request.
        self._stop_proxy()
        self.assertTrue(self.client.get('test_key') is None)

        # If we start the proxy again now, it'll reconnect immediately without any delay.
        self._start_proxy()
        self.assertEqual('test', self.client.get('test_key'))

        # Stop the proxy again, and make another request to cause the client to notice the
        # disconnection.
        self._stop_proxy()
        self.assertTrue(self.client.get('test_key') is None)

        # Make another request.  As above, the client will attempt a reconnection here, but
        # the server is still offline so it'll fail.  This will cause the retry delay to
        # kick in.
        # After the server comes back online, gets will continue to return None for 0.25
        # second, since delays are still deferred.
        self.assertTrue(self.client.get('test_key') is None)

        # Start the server.  This time, attempting to read from the server won't cause a
        # connection attempt, because we're still delaying.
        self._start_proxy()
        self.assertTrue(self.client.get('test_key') is None)

        # Sleep until the retry delay has elapsed, and verify that we connect to the server
        # this time.
        time.sleep(0.3)
        self.assertEqual('test', self.client.get('test_key'))

    def testGetMulti(self):
        six.assertCountEqual(self, self.client.set_multi({
            'test_key': 'value',
            'test_key2': 'value2'
        }), [])
        self.assertEqual({'test_key': 'value', 'test_key2': 'value2'},
                         self.client.get_multi(['test_key', 'test_key2']))

        self._stop_proxy()

        self.assertEqual({}, self.client.get_multi(['test_key', 'test_key2']))

        self._start_proxy()

        self.assertEqual({'test_key': 'value', 'test_key2': 'value2'},
                         self.client.get_multi(['test_key', 'test_key2']))

    def testDelete(self):
        self._stop_proxy()
        self.assertFalse(self.client.delete('test_key'))

    def testAdd(self):
        self._stop_proxy()
        self.assertFalse(self.client.add('test_key', 'test'))

    def testReplace(self):
        self._stop_proxy()
        self.assertFalse(self.client.replace('test_key', 'value2'))

    def testIncrement(self):
        self._stop_proxy()
        self.assertEqual(0, self.client.incr('test_key', 1))
        self.assertEqual(0, self.client.incr('test_key', 1))

    def testDecrement(self):
        self._stop_proxy()
        self.assertEqual(0, self.client.decr('test_key', 1))

    def testFlush(self):
        self._stop_proxy()
        self.assertTrue(self.client.flush_all())

    def testStats(self):
        self._stop_proxy()
        stats = self.client.stats()[self.server]
        self.assertEqual(stats, {})


class SocketMemcachedTests(MemcachedTests):
    """
    Same tests as above, just make sure it works with sockets.
    """

    def _server_host(self):
        return '/tmp/memcached.sock'
