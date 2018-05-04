VALUE_TABLE = {
    'true': True,
    'yes': True,
    'false': False,
    'no': False
}


class BooleanConverter(object):

    def convert(self, data):
        value = VALUE_TABLE.get(data.lower())

        if value is None:
            raise InvalidBooleanValue(data)
        return value


class InvalidBooleanValue(Exception):

    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value
