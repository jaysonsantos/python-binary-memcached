# TLS certificates for Memcached + TLS testing

Certificate generation is inspired by [badssl.com](https://github.com/chromium/badssl.com)

The certificates hierarchy is:

  - client-ca-root
    - client

  - ca-root
    - ca-intermediate
      - server-rsa2048

Use `make` to regen the certificates.