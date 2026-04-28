# Repository Guidelines

## Project Structure & Module Organization

This repository contains the `python-binary-memcached` package, a pure Python client for memcached's binary protocol.

- `bmemcached/`: package source. Core protocol code is in `protocol.py`; client implementations live in `bmemcached/client/`.
- `test/`: pytest suite. `conftest.py` starts local memcached processes on the standard port, port `5000`, a Unix socket, and IPv6.
- `docs/`: Sphinx documentation. `README.rst` is a symlink to `docs/intro.rst`.
- `setup.py`, `setup.cfg`, `tox.ini`: packaging, lint, and test configuration.

Generated artifacts such as `dist/`, `*.egg-info/`, and docs build outputs should not be edited as source.

## Build, Test, and Development Commands

- `python -m pip install -e . -r requirements_test.txt`: install the package in editable mode with test and lint dependencies.
- `pytest -s`: run the test suite. Requires the `memcached` executable available on `PATH`.
- `flake8`: run style checks with the repository's `max-line-length = 120`.
- `tox`: run the full configured gate for Python 3.8 through 3.12.
- `cd docs && make html`: build Sphinx HTML documentation into `docs/_build/html`.
- `python -m build --sdist --wheel --outdir dist/ .`: build release artifacts when the `build` package is installed.

## Coding Style & Naming Conventions

Use 4-space indentation for Python and 2-space indentation for YAML, matching `.editorconfig`. Trim trailing whitespace and keep a final newline. Follow existing module naming: lowercase Python modules, `test_*.py` test files, and descriptive test functions such as `test_server_parsing_ipv6`.

Prefer small changes that preserve the public client API. Keep compatibility with supported Python versions listed in `tox.ini` and `setup.py`.

## Testing Guidelines

Tests use pytest and pytest-cov. Add or update tests in `test/` for protocol changes, client behavior, socket handling, authentication, compression, TLS, and hashing behavior. Because tests spawn real memcached processes, verify memcached is installed before debugging failures.

Run `pytest -s` for focused local validation and `tox` before submitting cross-version changes.

## Commit & Pull Request Guidelines

Commits follow Commitizen conventional commits, enforced by the pre-commit `commit-msg` hook. Use forms like `fix: handle IPv6 server parsing`, `feat: add CAS return flag`, or `bump: version 0.31.3 -> 0.31.4`.

Pull requests should describe the behavior change, list test commands run, and link related issues when applicable. Include documentation updates for public API or usage changes.
