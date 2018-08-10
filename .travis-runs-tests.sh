#!/bin/bash
set -e
set -x
if [ "$STEP" = "tests" ]; then
    sudo service memcached stop
    py.test --version
    export PYTHONPATH=.
    python setup.py develop
    py.test --cov=bmemcached
fi

if [ "$STEP" = "lint" ]; then
    flake8
fi
