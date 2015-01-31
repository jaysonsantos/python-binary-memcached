class MemcachedException(Exception):
    pass


class AuthenticationNotSupported(MemcachedException):
    pass


class InvalidCredentials(MemcachedException):
    pass


class ValueTooBig(MemcachedException):
    pass
