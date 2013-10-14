Introduction to bmemcached
==========================

A pure python module (thread safe) to access memcached via it's binary with SASL auth support.

The main purpose of this module it to be able to communicate with memcached using binary protocol and support authentication, so it can work with Heroku for example.

Latest compiled docs on Read The Docs `here <https://python-binary-memcached.readthedocs.org>`_.

Installing
----------
Use pip or easy_install.

.. code-block:: bash

    pip install python-binary-memcached

Using
-----

.. code-block:: python

    import bmemcached
    client = bmemcached.Client(('127.0.0.1:11211', ), 'user',
                                'password')
    client.set('key', 'value')
    print client.get('key')


Running the tests
-----------------

First run memcached with:

.. code-block:: bash

    memcached -S -vvv
    memcached -p5000 -S -vvv
    memcached -S -s/tmp/memcached.sock -vvv

This is to cover all tests with socket, standard port and non standard port.

Then, run the tests.

.. code-block:: bash

    cd src_dir/
    py.test

Using with Django
-----------------
If you want to use it with Django, go to `django-bmemcached <https://github.com/jaysonsantos/django-bmemcached>`_ to get a Django backend.

Tests Status
------------
.. image:: https://travis-ci.org/jaysonsantos/python-binary-memcached.png?branch=master
    :target: https://travis-ci.org/jaysonsantos/python-binary-memcached
