#!/bin/bash
set -ex

if [ "$STEP" != "tests" ]; then
    exit 0
fi

sudo apt-get update
sudo apt install memcached