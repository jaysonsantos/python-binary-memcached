import six

__all__ = ('str_to_bytes', )


def str_to_bytes(value):
    """Simply convert a string type to bytes."""
    if isinstance(value, six.string_types):
        return value.encode()

    return value
