class MemcachedException(Exception):
    def __init__(self, message, code):
        super().__init__(message, code)
        self.message = message
        self.code = code

    def __str__(self):
        return f"{self.message} (code {self.code})"


class AuthenticationNotSupported(MemcachedException):
    pass


class InvalidCredentials(MemcachedException):
    pass
