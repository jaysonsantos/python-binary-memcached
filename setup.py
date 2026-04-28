import os
import sys

from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


version_dependant_requirements = [
    "uhashring < 2" if sys.version_info < (3, 6) else "uhashring",  # It uses f-strings
]

setup(
    name="python-binary-memcached",
    version="0.32.0",
    author="Jayson Reis",
    author_email="santosdosreis@gmail.com",
    description="A pure python module to access memcached via its binary protocol with SASL auth support",
    long_description=read("README.rst"),
    url="https://github.com/jaysonsantos/python-binary-memcached",
    packages=["bmemcached", "bmemcached.client"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    install_requires=[
        "six",
    ]
    + version_dependant_requirements,
)
