import os
import pytest
import subprocess
import ssl
import time

import bmemcached
import test_simple_functions


@pytest.yield_fixture(scope="module", autouse=True)
def memcached_tls():
    p = subprocess.Popen(
        [
            "memcached",
            "-p5001",
            "-Z",
            "-o",
            "ssl_chain_cert=test/certs/gen/chain/server-rsa2048.pem",
            "-o",
            "ssl_key=test/certs/gen/key/server-rsa2048.key",
            "-o",
            "ssl_ca_cert=test/certs/gen/crt/client-ca-root.crt",
            "-o",
            "ssl_verify_mode=1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.1)

    if p.poll() is not None:
        pytest.skip("Memcached server is not built with TLS support.")

    yield p
    p.kill()
    p.wait()


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
