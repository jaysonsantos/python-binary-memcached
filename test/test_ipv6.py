import bmemcached

from conftest import IPV6_PORT


SERVER = '[::1]:{}'.format(IPV6_PORT)


def test_set_and_get_over_ipv6():
    """
    Do a real round trip over IPv6.

    test/test_server_parsing.py only parses '::1' strings. It never opens a
    socket, so nothing proved that the IPv6 path works end to end.
    """
    client = bmemcached.Client(SERVER)
    try:
        assert client.set('ipv6_key', 'ipv6_value') is True
        assert client.get('ipv6_key') == 'ipv6_value'
    finally:
        client.delete('ipv6_key')
        client.disconnect_all()


def test_set_multi_and_get_multi_over_ipv6():
    client = bmemcached.Client(SERVER)
    try:
        assert client.set_multi({'ipv6_a': 1, 'ipv6_b': 'two'}) == []
        assert client.get_multi(['ipv6_a', 'ipv6_b']) == {'ipv6_a': 1, 'ipv6_b': 'two'}
    finally:
        client.delete_multi(['ipv6_a', 'ipv6_b'])
        client.disconnect_all()


def test_server_reports_the_ipv6_host():
    client = bmemcached.Client(SERVER)
    try:
        server = next(iter(client.servers))
        assert server.host == '::1'
        assert server.port == IPV6_PORT
    finally:
        client.disconnect_all()
