
class APIError(Exception):
    def __init__(self, error_code, message):
        super().__init__(message)
        self.error_code = error_code

class RateError(Exception):
    pass

