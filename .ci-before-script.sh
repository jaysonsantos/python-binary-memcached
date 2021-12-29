#!/bin/bash
set -ex

python -m pip install --upgrade pip
pip install -r requirements_test.txt && python setup.py develop

if [ "$STEP" != "tests" ]; then
    exit 0
fi

sudo apt-get update
sudo apt install memcached
