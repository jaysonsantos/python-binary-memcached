# Contributing

`python-binary-memcached` is a pure Python client for the memcached binary
protocol. This page shows how to report a bug, how to set up a development
environment, and how to send a pull request.

For deeper detail on the project layout, the coding style, and the test rules,
read [AGENTS.md](AGENTS.md).

## Report a bug

Open an issue at
[github.com/jaysonsantos/python-binary-memcached/issues](https://github.com/jaysonsantos/python-binary-memcached/issues).

Add this information:

- The version of `python-binary-memcached`, of Python, and of `memcached`.
- The client class and the constructor arguments that you use.
- The steps to reproduce the problem. A short code sample is best.
- The result that you expect and the result that you get.
- The full traceback, inside a fenced code block.

Do not report a security problem in a public issue. Send it by email to
Jayson Reis <santosdosreis@gmail.com>.

## Set up the development environment

You need Python 3.10 or later. You also need the `memcached` executable on your
`PATH`. The test suite starts real `memcached` processes. Build `memcached` with TLS
support. Without TLS support, the tests in `test/test_tls.py` skip.

Fork the repository, clone your fork, then install the package:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python -m pip install --group test --group lint
```

The `test` and `lint` dependency groups come from `pyproject.toml`.

The repository also has a Nix flake. If you use Nix, run `nix develop`. The
dev shell gives you Python, `ruff`, `pre-commit`, `commitizen`, and a
`memcached` build with TLS support.

Install the Git hooks once:

```bash
pre-commit install --install-hooks
pre-commit install --hook-type commit-msg
```

## Run tests and lint

```bash
pytest -s
ruff check .
ruff format --check .
```

`ruff` reads its configuration from `pyproject.toml`. The line length is 120.

Run the full gate for Python 3.10 to 3.14 before you send a cross-version
change. Install `tox` first, then run it:

```bash
python -m pip install tox
tox
```

## Write the commit message

Commits follow [Conventional Commits](https://www.conventionalcommits.org/).
Commitizen checks the message in the `commit-msg` hook. Use a type, a colon, and
a short summary in the imperative:

```
fix: handle IPv6 server parsing
feat: add CAS return flag
docs: add contributing guidelines
```

Keep the summary under 72 characters. Put the detail in the message body.

## Send a pull request

Open the pull request against the `main` branch. A good pull request has these
properties:

- It makes one change. Split unrelated work into separate pull requests.
- It keeps the public client API compatible.
- It adds or updates tests in `test/` for every behavior change.
- It passes `pytest -s`, `ruff check .`, and `ruff format --check .`.
- Its description states the behavior change and lists the commands that you ran.
- It links the related issue.
- It updates the documentation in `docs/` when the public API or the usage
  changes.

GitHub Actions runs the tests and the lint checks on every pull request. Fix a
red check before you ask for a review.
