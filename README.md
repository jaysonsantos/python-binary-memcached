# BMemcached
A pure python module to access memcached via it's binary with SASL auth support.

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