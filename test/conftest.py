import subprocess

import pytest
import time


@pytest.yield_fixture(scope='session', autouse=True)
def memcached_standard_port():
    p = subprocess.Popen(['memcached'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.1)
    yield p
    p.kill()
    p.wait()


@pytest.yield_fixture(scope='session', autouse=True)
def memcached_other_port():
    p = subprocess.Popen(['memcached', '-p5000'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.1)
    yield p
    p.kill()
    p.wait()


@pytest.yield_fixture(scope='session', autouse=True)
def memcached_socket():
    p = subprocess.Popen(['memcached', '-s/tmp/memcached.sock'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.1)
    yield p
    p.kill()
    p.wait()
