__all__ = ('str_to_bytes',)


def str_to_bytes(value):
    """
    Simply convert a string type to bytes if the value is a string
    and is an instance of str but not of bytes.
    struct.pack("<Q") is bytes but not a str.
    :param value:
    :param binary:
    :return:
    """
    if not isinstance(value, bytes) and isinstance(value, str):
        return value.encode()
    return value
