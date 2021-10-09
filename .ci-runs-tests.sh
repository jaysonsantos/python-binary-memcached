#!/bin/bash
set -ex
env
if [ "$STEP" = "tests" ]; then
    py.test --version
    export PYTHONPATH=.
    python setup.py develop
    py.test --cov=bmemcached
    exit 0
fi

if [ "$STEP" = "lint" ]; then
    flake8
    exit 0
fi

echo "Unknown step: $STEP"
exit 1
