import re
from abc import abstractmethod

from ingest.importer.conversion.data_converter import Converter
from ingest.importer.data_node import DataNode

SPLIT_FIELD_REGEX = '(?P<parent>\w*(\.\w*)*)\.(?P<target>\w*)'


class CellConversion(object):

    @abstractmethod
    def apply(self, data_node:DataNode, cell_data): ...


class DirectCellConversion(CellConversion):

    def __init__(self, field, converter:Converter):
        self.field = field
        self.converter = converter

    def apply(self, data_node:DataNode, cell_data):
        data_node[self.field] = self.converter.convert(cell_data)


class ListElementCellConversion(CellConversion):

    def __init__(self, field, converter:Converter):
        self.field = field
        self.converter = converter

    def apply(self, data_node:DataNode, cell_data):
        match = re.search(SPLIT_FIELD_REGEX, self.field)
        parent_path = match.group('parent')
        target_field = match.group('target')
        data = self.converter.convert(cell_data)
        data_node[parent_path] = [{target_field: data}]
