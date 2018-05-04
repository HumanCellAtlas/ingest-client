from abc import abstractmethod

VALUE_TABLE = {
    'true': True,
    'yes': True,
    'false': False,
    'no': False
}


class Converter:

    @abstractmethod
    def convert(self, data):
        return data


class BooleanConverter(Converter):

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


class ListConverter(Converter):
    def __init__(self, data_type='string'):
        self.data_type = data_type

    def convert(self, data):
        value = data.split('||')

        if self.data_type == 'integer':
            value = [int(elem) for elem in value]

        return value
