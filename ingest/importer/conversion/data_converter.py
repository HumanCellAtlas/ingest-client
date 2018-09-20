from abc import abstractmethod
from enum import Enum

from ingest.importer.conversion.exceptions import InvalidBooleanValue


class DataType(Enum):

    STRING = 'string'
    INTEGER = 'integer'
    BOOLEAN = 'boolean'
    UNDEFINED = 'undefined'

    @staticmethod
    def find(value: str):
        data_type = DataType.UNDEFINED
        if value is not None:
            try:
                data_type = DataType(value.lower())
            except ValueError:
                pass
        return data_type


class Converter:

    @abstractmethod
    def convert(self, data):
        raise NotImplementedError()


class DefaultConverter(Converter):

    def convert(self, data):
        return data


class StringConverter(Converter):

    def convert(self, data):
        return str(data)


class IntegerConverter(Converter):

    def convert(self, data):
        return int(data)


BOOLEAN_TABLE = {
    'true': True,
    'yes': True,
    'false': False,
    'no': False
}


class BooleanConverter(Converter):

    def convert(self, data):
        value = BOOLEAN_TABLE.get(data.lower())
        if value is None:
            raise InvalidBooleanValue(data)
        return value


CONVERTER_MAP = {

    DataType.STRING: StringConverter(),
    DataType.INTEGER: IntegerConverter(),
    DataType.BOOLEAN: BooleanConverter()

}


class ListConverter(Converter):

    def __init__(self, data_type: DataType=DataType.STRING, base_converter: Converter=None):
        self.base_type = data_type
        if base_converter is not None:
            self.base_type = DataType.UNDEFINED
            self.base_converter = base_converter
        else:
            self.base_converter = CONVERTER_MAP.get(data_type, CONVERTER_MAP[DataType.STRING])

    def convert(self, data):
        data = str(data)
        value = data.split('||')
        value = [self.base_converter.convert(elem) for elem in value]
        return value


DEFAULT = DefaultConverter()
