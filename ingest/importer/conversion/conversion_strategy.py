import re
from abc import abstractmethod

from ingest.importer.conversion import data_converter
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import Converter, ListConverter
from ingest.importer.conversion.exceptions import UnknownMainCategory
from ingest.importer.conversion.metadata_entity import MetadataEntity
from ingest.importer.conversion.utils import split_field_chain
from ingest.importer.data_node import DataNode

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
    def apply(self, metadata: MetadataEntity, cell_data): ...


class DirectCellConversion(CellConversion):

    def apply(self, metadata: MetadataEntity, cell_data):
        if cell_data is not None:
            content = self.converter.convert(cell_data)
            metadata.define_content(self.applied_field, content)


class ListElementCellConversion(CellConversion):

    def __init__(self, field: str, converter: Converter):
        list_converter = ListConverter(base_converter=converter)
        super(ListElementCellConversion, self).__init__(field, list_converter)

    def apply(self, metadata: MetadataEntity, cell_data):
        if cell_data is not None:
            parent_path, target_field = split_field_chain(self.applied_field)
            data_list = self.converter.convert(cell_data)
            parent = self._prepare_array(metadata, parent_path, len(data_list))
            for index, data in enumerate(data_list):
                target_object = parent[index]
                target_object[target_field] = data

    @staticmethod
    def _prepare_array(metadata, path, child_count):
        parent = metadata.get_content(path)
        if parent is None:
            parent = []
            metadata.define_content(path, parent)
        current_count = len(parent)
        missing_child_count = child_count - current_count
        if missing_child_count > 0:
            missing_children = [{} for _ in range(0, missing_child_count)]
            parent.extend(missing_children)
        return parent


class FieldOfSingleElementListCellConversion(CellConversion):

    def apply(self, metadata: MetadataEntity, cell_data):
        if cell_data is not None:
            parent_path, target_field = split_field_chain(self.applied_field)
            target_object = self._determine_target_object(metadata, parent_path)
            data = self.converter.convert(cell_data)
            target_object[target_field] = data

    @staticmethod
    def _determine_target_object(metadata, parent_path):
        parent = metadata.get_content(parent_path)
        if parent is None:
            target_object = {}
            parent = [target_object]
            metadata.define_content(parent_path, parent)
        else:
            target_object = parent[0]
        return target_object

class IdentityCellConversion(CellConversion):

    def apply(self, metadata: MetadataEntity, cell_data):
        value = self.converter.convert(cell_data)
        metadata.object_id = value
        metadata.define_content(self.applied_field, value)


class LinkedIdentityCellConversion(CellConversion):

    def __init__(self, field, main_category):
        super(LinkedIdentityCellConversion, self).__init__(field, _LIST_CONVERTER)
        self.main_category = main_category

    def apply(self, metadata: MetadataEntity, cell_data):
        if self.main_category is None:
            raise UnknownMainCategory()
        if cell_data is not None:
            links = self.converter.convert(cell_data)
            metadata.add_links(self.main_category, links)


class ExternalReferenceCellConversion(CellConversion):

    def __init__(self, field, main_category):
        super(ExternalReferenceCellConversion, self).__init__(field, _LIST_CONVERTER)
        self.main_category = main_category

    def apply(self, metadata: MetadataEntity, cell_data):
        link_ids = self.converter.convert(cell_data)
        metadata.add_external_links(self.main_category, link_ids)


class LinkingDetailCellConversion(CellConversion):

    def apply(self, metadata: MetadataEntity, cell_data):
        value = self.converter.convert(cell_data)
        metadata.define_linking_detail(self.applied_field, value)


class DoNothing(CellConversion):

    def __init__(self):
        super(DoNothing, self).__init__('', data_converter.DEFAULT)

    def apply(self, metadata: MetadataEntity, cell_data):
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
        elif ConversionType.LINKING_DETAIL == conversion_type:
            strategy = LinkingDetailCellConversion(field_name, converter)
        elif ConversionType.IDENTITY == conversion_type:
            strategy = IdentityCellConversion(field_name, converter)
        elif ConversionType.LINKED_IDENTITY == conversion_type:
            strategy = LinkedIdentityCellConversion(field_name, column_spec.main_category)
        elif ConversionType.EXTERNAL_REFERENCE == conversion_type:
            strategy = ExternalReferenceCellConversion(field_name, column_spec.main_category)
    return strategy
