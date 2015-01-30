import six

__all__ = ('long', )

if six.PY3:
    long = int
else:
	long = long
