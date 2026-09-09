class MemcachedException(Exception):
    def __init__(self, message, code):
        super(MemcachedException, self).__init__(message, code)
        self.message = message
        self.code = code

    def __str__(self):
        return '{} (code {})'.format(self.message, self.code)


class AuthenticationNotSupported(MemcachedException):
    pass


class InvalidCredentials(MemcachedException):
    pass
