import re
from abc import abstractmethod

from ingest.importer.conversion import data_converter
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import Converter, ListConverter
from ingest.importer.conversion.exceptions import UnknownMainCategory
from ingest.importer.conversion.utils import split_field_chain
from ingest.importer.data_node import DataNode

OBJECT_ID_FIELD = '_object_id'
CONTENT_FIELD = '_content'
LINKS_FIELD = '_links'
EXTERNAL_LINKS_FIELD = '_external_links'

_LIST_CONVERTER = ListConverter()


class CellConversion(object):

    def __init__(self, field, converter: Converter):
        self.field = field
        self.applied_field = self._process_applied_field(field)
        self.converter = converter

    @staticmethod
    def _process_applied_field(field):
        pattern = '(\w*\.){0,1}(?P<insert_field>.*)'
        match = re.match(pattern, field)
        return match.group('insert_field')

    @abstractmethod
    def apply(self, data_node:DataNode, cell_data): ...


class DirectCellConversion(CellConversion):

    def apply(self, data_node: DataNode, cell_data):
        if cell_data is not None:
            structured_field = f'{CONTENT_FIELD}.{self.applied_field}'
            data_node[structured_field] = self.converter.convert(cell_data)


class ListElementCellConversion(CellConversion):

    def apply(self, data_node:DataNode, cell_data):
        if cell_data is not None:
            structured_field = f'{CONTENT_FIELD}.{self.applied_field}'
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
        value = self.converter.convert(cell_data)
        data_node[OBJECT_ID_FIELD] = value
        structured_field = f'{CONTENT_FIELD}.{self.applied_field}'
        data_node[structured_field] = value


class LinkedIdentityCellConversion(CellConversion):

    def __init__(self, field, main_category):
        super(LinkedIdentityCellConversion, self).__init__(field, _LIST_CONVERTER)
        self.main_category = main_category

    def apply(self, data_node: DataNode, cell_data):
        if self.main_category is None:
            raise UnknownMainCategory()
        if cell_data is not None:
            linked_ids = self._get_linked_ids(data_node)
            linked_ids.extend(self.converter.convert(cell_data))

    def _get_linked_ids(self, data_node):
        links = self._get_links(data_node)
        entity_type = self.main_category
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


class ExternalReferenceCellConversion(CellConversion):

    def __init__(self, field, main_category):
        super(ExternalReferenceCellConversion, self).__init__(field, _LIST_CONVERTER)
        self.main_category = main_category

    def apply(self, data_node: DataNode, cell_data):
        data_node[f'{EXTERNAL_LINKS_FIELD}.account'] = [cell_data]


class DoNothing(CellConversion):

    def __init__(self):
        super(DoNothing, self).__init__('', data_converter.DEFAULT)

    def apply(self, data_node: DataNode, cell_data):
        pass


DO_NOTHING = DoNothing()


def determine_strategy(column_spec: ColumnSpecification):
    strategy = DO_NOTHING
    if column_spec is not None:
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
            strategy = LinkedIdentityCellConversion(field_name, column_spec.main_category)
        elif ConversionType.EXTERNAL_REFERENCE == conversion_type:
            strategy = ExternalReferenceCellConversion(field_name, column_spec.main_category)
    return strategy
