from bmemcached.client.constants import SOCKET_TIMEOUT

from .distributed import DistributedClient
from .replicating import ReplicatingClient

__all__ = ("Client", "DistributedClient", "ReplicatingClient")


# Keep compatibility with old versions
Client = ReplicatingClient
_SOCKET_TIMEOUT = SOCKET_TIMEOUT
