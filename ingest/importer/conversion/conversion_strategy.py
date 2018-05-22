from abc import abstractmethod

from ingest.importer.conversion.data_converter import Converter
from ingest.importer.conversion.column_specification import ColumnSpecification
from ingest.importer.conversion.utils import split_field_chain
from ingest.importer.data_node import DataNode


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
        parent_path, target_field = split_field_chain(self.field)
        target_object = self._determine_target_object(data_node, parent_path)
        data = self.converter.convert(cell_data)
        target_object[target_field] = data

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


class IdentityCellConversion(CellConversion):

    def apply(self, data_node: DataNode, cell_data):
        data_node['product_no_144'] = []


def determine_strategy(column_spec: ColumnSpecification):
    converter = column_spec.determine_converter()
    if column_spec.is_field_of_list_element():
        strategy = ListElementCellConversion(column_spec.field_name, converter)
    else:
        strategy = DirectCellConversion(column_spec.field_name, converter)
    return strategy
