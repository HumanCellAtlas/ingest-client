import re
from abc import abstractmethod

from ingest.importer.conversion.data_converter import Converter, CONVERTER_MAP, ListConverter
from ingest.importer.conversion.data_converter import DataType
from ingest.importer.data_node import DataNode

SPLIT_FIELD_REGEX = '(?P<parent>\w*(\.\w*)*)\.(?P<target>\w*)'


class ColumnSpecification:

    def __init__(self, field_name, raw_spec, parent=None):
        self.field_name = field_name
        self.data_type = DataType.find(raw_spec.get('value_type'))
        self.multivalue = bool(raw_spec.get('multivalue'))
        if parent is not None:
            self.field_of_list_member = bool(parent.get('multivalue'))

    def is_multivalue(self):
        return self.multivalue

    def is_field_of_list_member(self):
        return self.field_of_list_member

    def determine_converter(self):
        if not self.multivalue:
            default_converter = CONVERTER_MAP.get(DataType.STRING)
            converter = CONVERTER_MAP.get(self.data_type, default_converter)
        else:
            converter = ListConverter(self.data_type)
        return converter

    @staticmethod
    def build(field_name, data_type=DataType.UNDEFINED, multivalue=False):
        raw_spec = {
            'value_type': data_type.value,
            'multivalue': multivalue
        }
        return ColumnSpecification(field_name, raw_spec)


class CellConversion(object):

    def __init__(self, field, converter: Converter):
        self.field = field
        self.converter = converter

    @abstractmethod
    def apply(self, data_node:DataNode, cell_data): ...


class DirectCellConversion(CellConversion):

    def apply(self, data_node:DataNode, cell_data):
        data_node[self.field] = self.converter.convert(cell_data)


class ListElementCellConversion(CellConversion):

    def apply(self, data_node:DataNode, cell_data):
        parent_path, target_field = self._split_field_chain()
        target_object = self._determine_target_object(data_node, parent_path)
        data = self.converter.convert(cell_data)
        target_object[target_field] = data

    def _split_field_chain(self):
        match = re.search(SPLIT_FIELD_REGEX, self.field)
        parent_path = match.group('parent')
        target_field = match.group('target')
        return parent_path, target_field

    @staticmethod
    def _determine_target_object(data_node, parent_path):
        parent = data_node[parent_path]
        if parent is None:
            target_object = {}
            parent = [target_object]
            data_node[parent_path] = parent
        else:
            target_object = parent[0]
        return target_object


def determine_strategy(column_spec:ColumnSpecification):
    converter = column_spec.determine_converter()
    if column_spec.is_field_of_list_member():
        strategy = ListElementCellConversion(column_spec.field_name, converter)
    else:
        strategy = DirectCellConversion(column_spec.field_name, converter)
    return strategy
