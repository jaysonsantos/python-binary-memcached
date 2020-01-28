#!/bin/bash
set -ex

if [ "$STEP" != "tests" ]; then
    exit 0
fi

sudo apt-get update
sudo apt-get remove memcached
sudo apt-get build-dep memcached
sudo apt-get install libssl-dev
wget https://memcached.org/files/memcached-1.5.21.tar.gz
tar zxvf memcached-1.5.21.tar.gz
pushd memcached-1.5.21 && ./configure --prefix=/usr --enable-tls && make && sudo make install && popd
