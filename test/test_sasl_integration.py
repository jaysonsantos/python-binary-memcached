import pytest

import bmemcached
from bmemcached.exceptions import MemcachedException


def _client(memcached_sasl, username=None, password=None):
    port, default_user, default_password = memcached_sasl
    return bmemcached.Client(
        '127.0.0.1:{}'.format(port),
        default_user if username is None else username,
        default_password if password is None else password,
    )


def test_authenticate_then_set_and_get(memcached_sasl):
    """
    Authenticate against a real SASL-enabled memcached, then do a round trip.

    test/test_auth.py mocks Protocol._get_response and feeds canned bytes. It
    covers the client state machine only. Nothing exercised a real server with
    authentication turned on.
    """
    client = _client(memcached_sasl)
    try:
        assert client.set('sasl_key', 'sasl_value') is True
        assert client.get('sasl_key') == 'sasl_value'
    finally:
        client.delete('sasl_key')
        client.disconnect_all()


def test_wrong_password_is_rejected(memcached_sasl):
    """
    A wrong password fails the operation.

    memcached answers a failed SASL_AUTH with status 0x20, which the client
    does not map to InvalidCredentials. It maps 0x08 only. The client still
    refuses the operation, so this test asserts the base exception. The
    mapping gap is tracked separately.
    """
    client = _client(memcached_sasl, password='the-wrong-password')
    try:
        with pytest.raises(MemcachedException) as caught:
            client.set('sasl_key', 'sasl_value')
        assert 'Auth failure' in str(caught.value)
    finally:
        client.disconnect_all()


def test_protocol_authenticate_returns_true(memcached_sasl):
    port, username, password = memcached_sasl
    server = bmemcached.protocol.Protocol('127.0.0.1:{}'.format(port), username, password)
    try:
        assert server.authenticate(username, password) is True
        assert server.authenticated is True
    finally:
        server.disconnect()
