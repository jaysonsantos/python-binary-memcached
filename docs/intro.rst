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
    print(client.get('key'))


Using it with distributed keys

.. code-block:: python

    import bmemcached
    client = bmemcached.DistributedClient(
        ('127.0.0.1:11211', ), 'user', 'password'
    )
    client.set('key', 'value')
    print(client.get('key'))

Testing
-------

``python-binary-memcached`` unit tests are found in the ``test/`` directory
and are designed to be run using `pytest`_. `pytest`_ will discover the tests
automatically, so all you have to do is:

.. code-block:: console

    $ pytest
    ...
    170 passed in 4.43 seconds

This runs the tests with the default Python interpreter.

You can also verify that the tests pass on other supported Python interpreters.
For this we use ``tox``, which will automatically create a ``virtualenv`` for
each supported Python version and run the tests. For example:

.. code-block:: console

    $ tox
    ...
    py27: commands succeeded
    ERROR:  py34: InterpreterNotFound: python3.4
    py35: commands succeeded
    py36: commands succeeded
    py37: commands succeeded
    py38: commands succeeded

You may not have all the required Python versions installed, in which case you
will see one or more ``InterpreterNotFound`` errors.

Using with Django
-----------------
If you want to use it with Django, go to `django-bmemcached <https://github.com/jaysonsantos/django-bmemcached>`_ to get a Django backend.

Tests Status
------------
.. image:: https://travis-ci.org/jaysonsantos/python-binary-memcached.png?branch=master
    :target: https://travis-ci.org/jaysonsantos/python-binary-memcached

.. _`pytest`: https://pypi.org/project/pytest/
.. _`tox`: https://pypi.org/project/tox/
