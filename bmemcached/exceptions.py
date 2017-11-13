class MemcachedException(Exception):
    def __init__(self, message, code):
        self.code = code


class AuthenticationNotSupported(MemcachedException):
    pass


class InvalidCredentials(MemcachedException):
    pass
