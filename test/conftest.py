import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest


os.environ.setdefault("MEMCACHED_HOST", "localhost")

# How long to wait for a memcached process to accept a connection.
START_TIMEOUT = 10.0
POLL_INTERVAL = 0.02

SOCKET_PATH = "/tmp/memcached.sock"
IPV6_PORT = 5002
SASL_PORT = 5003


def _fail_reason(process, description):
    """
    Return a skip message when the process is not usable, else None.
    """
    if process.poll() is None:
        return None

    stderr = b""
    try:
        stderr = process.communicate(timeout=1)[1] or b""
    except subprocess.TimeoutExpired:  # pragma: no cover - defensive
        pass

    return "{} exited with code {}. {}".format(
        description, process.returncode, stderr.decode("utf8", "replace").strip()
    )


def _wait_until_accepting(process, description, connect):
    """
    Wait until ``connect`` succeeds, or skip with a clear reason.

    A fixed sleep is not enough. A slow start makes it flaky, and a memcached
    that never started at all gives an opaque ConnectionRefusedError in every
    test instead of one clear skip.
    """
    deadline = time.time() + START_TIMEOUT
    last_error = None

    while time.time() < deadline:
        reason = _fail_reason(process, description)
        if reason is not None:
            pytest.skip(reason)

        try:
            connect().close()
            return process
        except OSError as error:
            last_error = error
            time.sleep(POLL_INTERVAL)

    process.kill()
    process.wait()
    pytest.skip(
        "{} did not accept a connection within {:.0f}s. Last error: {}".format(
            description, START_TIMEOUT, last_error
        )
    )


def _start(args, description, connect):
    """
    Start a memcached process and wait for it to accept a connection.
    """
    try:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as error:
        pytest.skip("Cannot run {}: {}. Is memcached on PATH?".format(args[0], error))

    return _wait_until_accepting(process, description, connect)


def _stop(process):
    process.kill()
    process.wait()


def _tcp(host, port, family=socket.AF_INET):
    def connect():
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(POLL_INTERVAL * 10)
        sock.connect((host, port))
        return sock

    return connect


def _unix(path):
    def connect():
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(POLL_INTERVAL * 10)
        sock.connect(path)
        return sock

    return connect


@pytest.fixture(scope="session", autouse=True)
def memcached_standard_port():
    process = _start(["memcached"], "memcached on port 11211", _tcp("127.0.0.1", 11211))
    yield process
    _stop(process)


@pytest.fixture(scope="session", autouse=True)
def memcached_other_port():
    process = _start(["memcached", "-p5000"], "memcached on port 5000", _tcp("127.0.0.1", 5000))
    yield process
    _stop(process)


@pytest.fixture(scope="session", autouse=True)
def memcached_socket():
    # A previous run that died before teardown leaves the socket file behind.
    # memcached then fails to bind.
    _remove_socket_file()

    process = _start(
        ["memcached", "-s" + SOCKET_PATH],
        "memcached on unix socket {}".format(SOCKET_PATH),
        _unix(SOCKET_PATH),
    )
    yield process
    _stop(process)
    _remove_socket_file()


def _remove_socket_file():
    try:
        os.unlink(SOCKET_PATH)
    except FileNotFoundError:
        pass


@pytest.fixture(scope="session", autouse=True)
def memcached_ipv6():
    # This server needs its own port. On Linux a plain memcached binds both
    # INADDR_ANY and IN6ADDR_ANY, so port 11211 is already taken here.
    process = _start(
        ["memcached", "-l::1", "-p{}".format(IPV6_PORT)],
        "memcached on [::1]:{}".format(IPV6_PORT),
        _tcp("::1", IPV6_PORT, socket.AF_INET6),
    )
    yield process
    _stop(process)


@pytest.fixture(scope="session")
def memcached_sasl():
    """
    Start a memcached with SASL authentication enabled.

    This fixture yields (port, username, password). It skips when memcached is
    not built with SASL support, or when saslpasswd2 is absent from PATH.
    Cyrus SASL needs a real user database, and saslpasswd2 is the tool that
    writes one.
    """
    if shutil.which("saslpasswd2") is None:
        pytest.skip("saslpasswd2 is not on PATH. Cannot build a SASL user database.")

    username = "bmemcached_test_user"
    password = "bmemcached_test_password"
    # Cyrus SASL stores the user under a realm. memcached looks it up under the
    # local hostname, so both sides must agree.
    realm = socket.gethostname()

    conf_dir = tempfile.mkdtemp(prefix="bmemcached-sasl-")
    sasldb_path = os.path.join(conf_dir, "sasldb2")

    with open(os.path.join(conf_dir, "memcached.conf"), "w") as conf:
        conf.write(
            "mech_list: PLAIN\n"
            "pwcheck_method: auxprop\n"
            "auxprop_plugin: sasldb\n"
            "sasldb_path: {}\n".format(sasldb_path)
        )

    written = subprocess.run(
        ["saslpasswd2", "-p", "-c", "-f", sasldb_path, "-a", "memcached", "-u", realm, username],
        input=password.encode(),
        capture_output=True,
    )
    if written.returncode != 0:
        shutil.rmtree(conf_dir, ignore_errors=True)
        pytest.skip(
            "saslpasswd2 failed: {}".format(written.stderr.decode("utf8", "replace").strip())
        )

    environment = dict(os.environ, SASL_CONF_PATH=conf_dir)
    try:
        process = subprocess.Popen(
            ["memcached", "-p{}".format(SASL_PORT), "-S"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
    except OSError as error:
        shutil.rmtree(conf_dir, ignore_errors=True)
        pytest.skip("Cannot run memcached: {}. Is memcached on PATH?".format(error))

    try:
        _wait_until_accepting(
            process,
            "memcached with SASL on port {}".format(SASL_PORT),
            _tcp("127.0.0.1", SASL_PORT),
        )
        yield SASL_PORT, username, password
    finally:
        _stop(process)
        shutil.rmtree(conf_dir, ignore_errors=True)
