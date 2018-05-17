import re
from abc import abstractmethod

from ingest.importer.conversion.data_converter import Converter, DataType
from ingest.importer.data_node import DataNode

SPLIT_FIELD_REGEX = '(?P<parent>\w*(\.\w*)*)\.(?P<target>\w*)'


class ColumnSpecification:

    def __init__(self, raw_spec, parent=None):
        self.data_type = DataType.find(raw_spec.get('value_type'))
        self.multivalue = bool(raw_spec.get('multivalue'))
        if parent is not None:
            self.field_of_list_member = bool(parent.get('multivalue'))

    def is_multivalue(self):
        return self.multivalue

    def is_field_of_list_member(self):
        return self.field_of_list_member


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
