class InvalidBooleanValue(Exception):

    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value

    def __str__(self):
        return f'Invalid Boolean Value: {self.value}'


class UnknownMainCategory(Exception):

    DEFAULT_MESSAGE = 'Main category was not specified.'

    def __init__(self, message=DEFAULT_MESSAGE):
        self.message = message

    def __str__(self):
        return repr(self.message)
