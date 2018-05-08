from abc import abstractmethod
from enum import Enum

from ingest.importer.conversion.exceptions import InvalidBooleanValue

VALUE_TABLE = {
    'true': True,
    'yes': True,
    'false': False,
    'no': False
}


class DataType(Enum):
    STRING = 'string',
    INTEGER = 'integer'


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


class ListConverter(Converter):

    def __init__(self, data_type:DataType=DataType.STRING):
        self.data_type = data_type

    def convert(self, data):
        value = data.split('||')

        if self.data_type == DataType.INTEGER:
            value = [int(elem) for elem in value]

        return value
