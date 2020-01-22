import os
import subprocess
import time

import pytest


os.environ.setdefault("MEMCACHED_HOST", "localhost")


@pytest.yield_fixture(scope="session", autouse=True)
def memcached_standard_port():
    p = subprocess.Popen(
        ["memcached"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(0.1)
    yield p
    p.kill()
    p.wait()


@pytest.yield_fixture(scope="session", autouse=True)
def memcached_other_port():
    p = subprocess.Popen(
        ["memcached", "-p5000"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(0.1)
    yield p
    p.kill()
    p.wait()


@pytest.yield_fixture(scope="session", autouse=True)
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
    yield p
    p.kill()
    p.wait()


@pytest.yield_fixture(scope="session", autouse=True)
def memcached_socket():
    p = subprocess.Popen(
        ["memcached", "-s/tmp/memcached.sock"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.1)
    yield p
    p.kill()
    p.wait()
