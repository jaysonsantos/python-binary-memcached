#!/bin/bash
sudo service memcached stop
py.test --version
export PYTHONPATH=.
python setup.py develop
py.test --cov=bmemcached
exit $?
