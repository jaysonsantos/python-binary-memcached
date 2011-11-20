# BMemcached
A pure python module to access memcached via it's binary with SASL auth support.

The main purpose of this module it to be able to communicate with memcached using binary protocol and support authentication, so it can work with Heroku for example.

## Running the tests
First run memcached with:

```bash
memcached -S -vvv
```

Then, run the tests.

```bash
cd src_dir/
nosetests
```