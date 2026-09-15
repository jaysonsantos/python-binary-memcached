# Security Policy

`python-binary-memcached` is a pure Python client for the memcached binary protocol.
It handles SASL authentication, passwords, and TLS connections.
The maintainer takes vulnerability reports for this code seriously.

## Supported Versions

The maintainer ships security fixes only for the latest release line.
Older release lines do not get security fixes.

| Version | Supported |
| ------- | --------- |
| 0.32.x  | Yes       |
| < 0.32  | No        |

Install the latest release from PyPI before you report a problem.
The package requires Python 3.10 or later.
The project tests Python 3.10 through Python 3.14.

## How to Report a Vulnerability

Do not open a public issue for a vulnerability.
A public issue tells attackers about the problem before a fix exists.

Report a vulnerability in one of these two ways:

1. Use GitHub private vulnerability reporting. Open
   <https://github.com/jaysonsantos/python-binary-memcached/security/advisories/new>
   and complete the form. This method is preferred.
2. Send an email to <santosdosreis@gmail.com>. Use this method if the GitHub form
   is not available to you.

Add this information to your report:

- The affected version of `python-binary-memcached`.
- Your Python version and your operating system.
- The memcached server version and the configuration, if they are relevant.
- The steps to reproduce the problem.
- The impact that you expect.
- A proof of concept or a test case, if you have one.

Do not include real passwords, real SASL credentials, or real TLS private keys.
Replace them with placeholder values.

## What Happens Next

- The maintainer sends an acknowledgement in a few working days.
- The maintainer confirms the problem and finds the affected versions.
- The maintainer writes a fix and a test.
- The maintainer ships the fix in a new release on PyPI.
- The maintainer publishes a GitHub security advisory with a credit to you.

Tell the maintainer if you want no credit in the advisory.
This project is volunteer work, so the maintainer gives no fixed response deadline.
The maintainer keeps you informed about the progress of your report.

## Scope

This policy covers the code in this repository.
It does not cover the memcached server, the operating system, or other packages.
Report a problem in the memcached server to the memcached project.
