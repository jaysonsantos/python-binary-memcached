import os
from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


setup(
    name='python-binary-memcached',
    version='0.30.1',
    author='Jayson Reis',
    author_email='santosdosreis@gmail.com',
    description='A pure python module to access memcached via its binary protocol with SASL auth support',
    long_description='{0}\n{1}'.format(read('README.rst'), read('CHANGELOG.rst')),
    url='https://github.com/jaysonsantos/python-binary-memcached',
    packages=['bmemcached', 'bmemcached.client'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=[
        'six',
        'uhashring',
    ]
)
