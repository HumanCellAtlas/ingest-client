from abc import abstractmethod

from ingest.importer.conversion import data_converter, utils
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import Converter
from ingest.importer.conversion.utils import split_field_chain
from ingest.importer.data_node import DataNode

OBJECT_ID_FIELD = '_object_id'
CONTENT_FIELD = '_content'
LINKS_FIELD = '_links'


class CellConversion(object):

    def __init__(self, field, converter: Converter):
        self.field = field
        self.converter = converter

    @abstractmethod
    def apply(self, data_node:DataNode, cell_data): ...


class DirectCellConversion(CellConversion):

    def apply(self, data_node:DataNode, cell_data):
        if cell_data is None:
            return

        field_chain = utils.get_field_chain(self.field)
        structured_field = f'{CONTENT_FIELD}.{field_chain}'
        data_node[structured_field] = self.converter.convert(cell_data)


class ListElementCellConversion(CellConversion):

    def apply(self, data_node:DataNode, cell_data):
        if cell_data is None:
            return
        field_chain = utils.get_field_chain(self.field)
        structured_field = f'{CONTENT_FIELD}.{field_chain}'
        parent_path, target_field = split_field_chain(structured_field)
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
        if cell_data is None:
            return
        converted_data = self.converter.convert(cell_data)
        data_node[OBJECT_ID_FIELD] = converted_data

        field_chain = utils.get_field_chain(self.field)
        structured_field = f'{CONTENT_FIELD}.{field_chain}'
        data_node[structured_field] = converted_data


class LinkedIdentityCellConversion(CellConversion):

    def apply(self, data_node: DataNode, cell_data):
        if cell_data is None:
            return
        linked_ids = self._get_linked_ids(data_node)
        linked_ids.append(self.converter.convert(cell_data))

    def _get_linked_ids(self, data_node):
        links = self._get_links(data_node)
        entity_type = utils.extract_root_field(self.field)
        linked_ids = links.get(entity_type)
        if not linked_ids:
            linked_ids = []
            links[entity_type] = linked_ids
        return linked_ids

    @staticmethod
    def _get_links(data_node):
        links = data_node[LINKS_FIELD]
        if not links:
            links = {}
            data_node[LINKS_FIELD] = links
        return links


class DoNothing(CellConversion):

    def __init__(self):
        super(DoNothing, self).__init__('', data_converter.DEFAULT)

    def apply(self, data_node: DataNode, cell_data):
        pass


def determine_strategy(column_spec: ColumnSpecification):
    field_name = column_spec.field_name
    converter = column_spec.determine_converter()
    conversion_type = column_spec.get_conversion_type()
    if ConversionType.MEMBER_FIELD == conversion_type:
        strategy = DirectCellConversion(field_name, converter)
    elif ConversionType.FIELD_OF_LIST_ELEMENT == conversion_type:
        strategy = ListElementCellConversion(field_name, converter)
    elif ConversionType.IDENTITY == conversion_type:
        strategy = IdentityCellConversion(field_name, converter)
    elif ConversionType.LINKED_IDENTITY == conversion_type:
        strategy = LinkedIdentityCellConversion(field_name, converter)
    else:
        strategy = DoNothing()
    return strategy

