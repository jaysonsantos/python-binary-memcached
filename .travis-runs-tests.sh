#!/bin/bash -e
if [ "$STEP" -eq "tests" ]; then
    sudo service memcached stop
    py.test --version
    export PYTHONPATH=.
    python setup.py develop
    py.test --cov=bmemcached
fi

if [ "$STEP" -eq "lint" ]; then
    flake8
fi
