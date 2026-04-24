import warnings

from bmemcached.client.mixin import ClientMixin


class ReplicatingClient(ClientMixin):
    """
    This is intended to be a client class which implement standard cache interface that common libs do...

    It replicates values over servers and get a response from the first one it can.

    .. warning::
        CAS operations are fundamentally incompatible with multi-server
        replication. Each server maintains its own independent CAS counter,
        so a CAS value obtained from one replica will not match any other
        replica. As a consequence:

        * :meth:`cas` against more than one replica causes at most one
          server to accept the write; the rest silently reject it, leaving
          the replicas divergent. The same hazard applies to
          :meth:`set_multi` mappings that use ``(key, cas)`` tuple keys.
        * :meth:`gets`, :meth:`get` with ``get_cas=True``, and
          :meth:`get_multi` with ``get_cas=True`` return a CAS from
          whichever replica happens to respond first. That value cannot
          be safely passed back to :meth:`cas` on a multi-replica client,
          for the reason above.

        If you need CAS semantics, configure this client with exactly one
        server (or use :class:`DistributedClient`).
    """

    def _warn_multi_replica_cas(self, op, hazard):
        if len(self._servers) > 1:
            warnings.warn(
                "{} on a ReplicatingClient with more than one server {}. "
                "See the class docstring.".format(op, hazard),
                UserWarning,
                stacklevel=3,
            )

    def _set_retry_delay(self, value):
        for server in self._servers:
            server.set_retry_delay(value)

    def enable_retry_delay(self, enable):
        """
        Enable or disable delaying between reconnection attempts.

        The first reconnection attempt will always happen immediately, so intermittent network
        errors don't cause caching to turn off.  The retry delay takes effect after the first
        reconnection fails.

        The reconnection delay is enabled by default for TCP connections, and disabled by
        default for Unix socket connections.
        """
        # The public API only allows enabling or disabling the delay, so it'll be easier to
        # add exponential falloff in the future.  _set_retry_delay is exposed for tests.
        self._set_retry_delay(5 if enable else 0)

    def get(self, key, default=None, get_cas=False):
        """
        Get a key from server.

        .. warning::
            When called with ``get_cas=True`` against more than one replica,
            the returned CAS is from whichever replica responded first and
            cannot be safely passed to :meth:`cas` on this client. See the
            class-level note on CAS and replication.

        :param key: Key's name
        :type key: six.string_types
        :param default: In case memcached does not find a key, return a default value
        :param get_cas: If true, return (value, cas), where cas is the new CAS value.
        :type get_cas: boolean
        :return: Returns a key data from server.
        :rtype: object
        """
        if get_cas:
            self._warn_multi_replica_cas(
                "get(get_cas=True)",
                "returns a CAS that cannot be safely passed back to cas() on this client",
            )
        for server in self.servers:
            value, cas = server.get(key)
            if value is not None:
                if get_cas:
                    return value, cas
                else:
                    return value
        if default is not None:
            if get_cas:
                return default, None
            return default
        if get_cas:
            return None, None

    def gets(self, key):
        """
        Get a key from server, returning the value and its CAS key.

        This method is for API compatibility with other implementations.

        .. warning::
            Against more than one replica, the returned CAS is from
            whichever replica responded first and cannot be safely passed
            to :meth:`cas` on this client. See the class-level note on
            CAS and replication.

        :param key: Key's name
        :type key: six.string_types
        :return: Returns (key data, value), or (None, None) if the value is not in cache.
        :rtype: object
        """
        self._warn_multi_replica_cas(
            "gets()",
            "returns a CAS that cannot be safely passed back to cas() on this client",
        )
        for server in self.servers:
            value, cas = server.get(key)
            if value is not None:
                return value, cas
        return None, None

    def get_multi(self, keys, get_cas=False):
        """
        Get multiple keys from server.

        .. warning::
            When called with ``get_cas=True`` against more than one replica,
            each key's returned CAS is from whichever replica returned that
            key first; none of those values can be safely passed to
            :meth:`cas` on this client. See the class-level note on CAS
            and replication.

        :param keys: A list of keys to from server.
        :type keys: list
        :param get_cas: If get_cas is true, each value is (data, cas), with each result's CAS value.
        :type get_cas: boolean
        :return: A dict with all requested keys.
        :rtype: dict
        """
        if get_cas:
            self._warn_multi_replica_cas(
                "get_multi(get_cas=True)",
                "returns CAS values that cannot be safely passed back to cas() on this client",
            )
        d = {}
        if keys:
            for server in self.servers:
                results = server.get_multi(keys)
                if not get_cas:
                    # Remove CAS data
                    for key, (value, cas) in results.items():
                        results[key] = value
                d.update(results)
                keys = [_ for _ in keys if _ not in d]
                if not keys:
                    break
        return d

    def set(self, key, value, time=0, compress_level=-1):
        """
        Set a value for a key on server.

        :param key: Key's name
        :type key: str
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :param compress_level: How much to compress.
            0 = no compression, 1 = fastest, 9 = slowest but best,
            -1 = default compression level.
        :type compress_level: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        returns = []
        for server in self.servers:
            returns.append(server.set(key, value, time, compress_level=compress_level))

        return any(returns)

    def cas(self, key, value, cas, time=0, compress_level=-1):
        """
        Set a value for a key on server if its CAS value matches cas.

        .. warning::
            See the class-level note on CAS and replication. Each replica has
            its own CAS counter, so a single CAS value cannot match on more
            than one server. Calling this against multiple replicas will
            silently diverge them -- at most one replica accepts the write.

        :param key: Key's name
        :type key: six.string_types
        :param value: A value to be stored on server.
        :type value: object
        :param cas: The CAS value previously obtained from a call to get*.
        :type cas: int
        :param time: Time in seconds that your key will expire.
        :type time: int
        :param compress_level: How much to compress.
            0 = no compression, 1 = fastest, 9 = slowest but best,
            -1 = default compression level.
        :type compress_level: int
        :return: True in case of success and False in case of failure
        :rtype: bool
        """
        self._warn_multi_replica_cas(
            "cas()",
            "will silently diverge replicas: at most one server can match a given CAS",
        )
        returns = []
        for server in self.servers:
            returns.append(server.cas(key, value, cas, time, compress_level=compress_level))

        return any(returns)

    def set_multi(self, mappings, time=0, compress_level=-1):
        """
        Set multiple keys with it's values on server.

        .. warning::
            If any key is given as a ``(key, cas)`` tuple, the same CAS-plus-
            replication hazard documented on :meth:`cas` applies: the CAS
            value can match at most one replica, so those entries will
            silently diverge across servers.

        :param mappings: A dict with keys/values
        :type mappings: dict
        :param time: Time in seconds that your key will expire.
        :type time: int
        :param compress_level: How much to compress.
            0 = no compression, 1 = fastest, 9 = slowest but best,
            -1 = default compression level.
        :type compress_level: int
        :return: List of keys that failed to be set on any server.
        :rtype: list
        """
        if len(self._servers) > 1 and any(isinstance(k, tuple) for k in mappings):
            self._warn_multi_replica_cas(
                "set_multi() with (key, cas) tuple keys",
                "will silently diverge replicas for those entries: at most one server can match a given CAS",
            )
        returns = set()
        if mappings:
            for server in self.servers:
                returns |= set(server.set_multi(mappings, time, compress_level=compress_level))

        return list(returns)

    def add(self, key, value, time=0, compress_level=-1):
        """
        Add a key/value to server ony if it does not exist.

        :param key: Key's name
        :type key: six.string_types
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :param compress_level: How much to compress.
            0 = no compression, 1 = fastest, 9 = slowest but best,
            -1 = default compression level.
        :type compress_level: int
        :return: True if key is added False if key already exists
        :rtype: bool
        """
        returns = []
        for server in self.servers:
            returns.append(server.add(key, value, time, compress_level=compress_level))

        return any(returns)

    def replace(self, key, value, time=0, compress_level=-1):
        """
        Replace a key/value to server ony if it does exist.

        :param key: Key's name
        :type key: six.string_types
        :param value: A value to be stored on server.
        :type value: object
        :param time: Time in seconds that your key will expire.
        :type time: int
        :param compress_level: How much to compress.
            0 = no compression, 1 = fastest, 9 = slowest but best,
            -1 = default compression level.
        :type compress_level: int
        :return: True if key is replace False if key does not exists
        :rtype: bool
        """
        returns = []
        for server in self.servers:
            returns.append(server.replace(key, value, time, compress_level=compress_level))

        return any(returns)

    def delete(self, key, cas=0):
        """
        Delete a key/value from server. If key does not exist, it returns True.

        :param key: Key's name to be deleted
        :param cas: CAS of the key
        :return: True in case o success and False in case of failure.
        """
        returns = []
        for server in self.servers:
            returns.append(server.delete(key, cas))

        return any(returns)

    def delete_multi(self, keys):
        returns = []
        for server in self.servers:
            returns.append(server.delete_multi(keys))

        return all(returns)

    def incr(self, key, value, default=0, time=1000000):
        """
        Increment a key, if it exists, returns it's actual value, if it don't, return 0.

        :param key: Key's name
        :type key: six.string_types
        :param value: Number to be incremented
        :type value: int
        :param default: If key not set, initialize to this value
        :type default: int
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: Actual value of the key on server
        :rtype: int
        """
        returns = []
        for server in self.servers:
            returns.append(server.incr(key, value, default=default, time=time))

        return returns[0]

    def decr(self, key, value, default=0, time=1000000):
        """
        Decrement a key, if it exists, returns it's actual value, if it don't, return 0.
        Minimum value of decrement return is 0.

        :param key: Key's name
        :type key: six.string_types
        :param value: Number to be decremented
        :type value: int
        :param default: If key not set, initialize to this value
        :type default: int
        :param time: Time in seconds that your key will expire.
        :type time: int
        :return: Actual value of the key on server
        :rtype: int
        """
        returns = []
        for server in self.servers:
            returns.append(server.decr(key, value, default=default, time=time))

        return returns[0]
