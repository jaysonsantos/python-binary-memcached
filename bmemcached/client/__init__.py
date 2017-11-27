from bmemcached.client.constants import SOCKET_TIMEOUT

from .replicating import ReplicatingClient


# Keep compatibility with old versions
Client = ReplicatingClient
_SOCKET_TIMEOUT = SOCKET_TIMEOUT
