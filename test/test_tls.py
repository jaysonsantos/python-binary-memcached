import os
import pytest
import subprocess
import ssl
import time
import trustme

import bmemcached
import test_simple_functions


ca = trustme.CA()
server_cert = ca.issue_cert(os.environ["MEMCACHED_HOST"] + u"")


@pytest.yield_fixture(scope="module", autouse=True)
def memcached_tls():
    key = server_cert.private_key_pem
    cert = server_cert.cert_chain_pems[0]

    with cert.tempfile() as c, key.tempfile() as k:
        p = subprocess.Popen(
            [
                "memcached",
                "-p5001",
                "-Z",
                "-o",
                "ssl_key={}".format(k),
                "-o",
                "ssl_chain_cert={}".format(c),
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
        ctx = ssl.create_default_context()

        ca.configure_trust(ctx)

        self.server = "{}:5001".format(os.environ["MEMCACHED_HOST"])
        self.client = bmemcached.Client(self.server, tls_context=ctx)
        self.reset()
