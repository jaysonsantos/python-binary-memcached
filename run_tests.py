#!/usr/bin/env python
from __future__ import print_function

import subprocess
import sys

import pytest


print('Executing memcached servers')
standard_port = subprocess.Popen(['memcached'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
another_port = subprocess.Popen(['memcached', '-p5000'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
socketed = subprocess.Popen(['memcached', '-s/tmp/memcached.sock'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print('Starting py.test')
return_code = pytest.main(sys.argv[1:])

print('\nKilling memcached servers')
for p in (standard_port, another_port, socketed):
    print('Killing {}'.format(p.pid))
    p.kill()

exit(return_code)
