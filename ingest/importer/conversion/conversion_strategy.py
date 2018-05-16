from abc import abstractmethod

from ingest.importer.conversion.data_converter import Converter
from ingest.importer.data_node import DataNode


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
        pass

    def apply(self, data_node:DataNode, cell_data):
        data_node['list_of_things'] = {}
