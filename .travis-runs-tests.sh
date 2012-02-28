#!/bin/bash
sudo service memcached stop
memcached -S -d
memcached -s/tmp/memached.sock -d
memcached -p5000 -d

nosetests
exit $?
