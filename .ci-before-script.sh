#!/bin/bash
set -ex

python -m pip install --upgrade pip
pip install -e . && pip install --group test

if [ "$STEP" = "lint" ]; then
    pip install --group lint
fi

if [ "$STEP" != "tests" ]; then
    exit 0
fi

sudo apt-get update
sudo apt-get install -y memcached
