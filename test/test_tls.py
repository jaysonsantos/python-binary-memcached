import os
import ssl

import bmemcached
import test_simple_functions


class TLSMemcachedTests(test_simple_functions.MemcachedTests):
    """
    Same tests as above, just make sure it works with TLS.
    """

    def setUp(self):
        ctx = ssl.create_default_context(
            cafile="test/certs/gen/crt/ca-root.crt"
        )

        self.server = "{}:5001".format(os.environ["MEMCACHED_HOST"])
        self.client = bmemcached.Client(self.server, tls_context=ctx)
        self.reset()
