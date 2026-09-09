#!/bin/bash
set -ex
env
if [ "$STEP" = "tests" ]; then
    py.test --version
    export PYTHONPATH=.
    pip install -e .
    py.test --cov=bmemcached
    exit 0
fi

if [ "$STEP" = "lint" ]; then
    flake8
    exit 0
fi

echo "Unknown step: $STEP"
exit 1
