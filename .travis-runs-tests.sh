#!/bin/bash
sudo service memcached stop
py.test --version
PYTHONPATH=. py.test --cov=bmemcached
exit $?
