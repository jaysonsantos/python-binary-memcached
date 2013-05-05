import logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

import concurrent.futures

import bmemcached

c = bmemcached.Client('127.0.0.1:11211')


def get(key):
    return c.get(key)

with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
    f = [executor.submit(get, '12345690') for i in xrange(20)]

    for future in concurrent.futures.as_completed(f):
        print future.result()
