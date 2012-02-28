#!/bin/bash
sudo service memcached stop
memcached -S -d
memached -s/tmp/memached.sock -d
memached -p5000 -d

nosetests
exit $?
