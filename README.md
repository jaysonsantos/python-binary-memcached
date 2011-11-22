# BMemcached
A pure python module to access memcached via it's binary with SASL auth support.

The main purpose of this module it to be able to communicate with memcached using binary protocol and support authentication, so it can work with Heroku for example.

## Installing
Use pip or easy_install.

```python
pip install python-binary-memcached
```

## Using

```python
import bmemcached
client = bmemcached.Client(('127.0.0.1:11211', ), 'user',
            'password')
client.set('key', 'value')
print client.get('key')
```

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

## Using with Django
If you want to use it with Django, go to [django-bmemcached] (https://github.com/jaysonsantos/django-bmemcached) to get a Django backend.