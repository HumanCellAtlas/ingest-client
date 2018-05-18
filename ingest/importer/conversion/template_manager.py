import re

import copy
from openpyxl.worksheet import Worksheet

from ingest.importer.conversion import utils, conversion_strategy
from ingest.importer.conversion.column_specification import ColumnSpecification
from ingest.importer.conversion.conversion_strategy import CellConversion, DirectCellConversion
from ingest.importer.conversion.data_converter import ListConverter, DataType, CONVERTER_MAP, \
    Converter
from ingest.importer.data_node import DataNode
from ingest.template.schema_template import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def create_template_node(self, worksheet:Worksheet):
        tab_spec = self.template.get_tab_spec(worksheet.title)
        data_node = DataNode()
        data_node['describedBy'] = tab_spec['schema']['url']
        data_node['schema_type'] = tab_spec['schema']['domain_entity']
        return data_node

    def create_row_template(self, worksheet:Worksheet):
        for row in worksheet.iter_rows(row_offset=3, max_row=1):
            header_row = row
        cell = header_row[0]
        header = cell.value
        parent_path, __ = utils.split_field_chain(header)
        raw_spec = self.template.get_key_for_label(header)
        raw_parent_spec = self.template.get_key_for_label(parent_path)
        column_spec = ColumnSpecification.build_raw(header, raw_spec, parent=raw_parent_spec)
        strategy = conversion_strategy.determine_strategy(column_spec)
        return RowTemplate([strategy])

    # TODO deprecate this! Logic is now moved to ColumnSpecification
    def get_converter(self, header_name):
        column_spec = self.template.lookup(header_name)
        default_converter = CONVERTER_MAP[DataType.STRING]

        if not column_spec:
            return default_converter

        data_type = self._resolve_data_type(column_spec)
        if column_spec.get('multivalue', False):
            converter = ListConverter(data_type=data_type)
        else:
            converter = CONVERTER_MAP.get(data_type, default_converter)
        return converter

    def _resolve_data_type(self, column_spec):
        value_type = column_spec.get('value_type')
        data_type = DataType.find(value_type)
        return data_type

    def is_parent_field_multivalue(self, header_name):
        parent_field = self._get_parent_field(header_name)
        column_spec = self.template.lookup(parent_field)

        return column_spec and column_spec.get('multivalue') and (
            column_spec.get('value_type') and column_spec.get('value_type') == 'object'
        )

    def _get_parent_field(self, header_name):
        match = re.search('(?P<field_chain>.*)(\.\w+)', header_name)
        parent_field = match.group('field_chain')
        return parent_field

    def get_schema_url(self, concrete_entity):
        schema = self._get_schema(concrete_entity)
        # TODO must query schema endpoint in core to get the latest version
        return schema.get('url') if schema else None

    def get_domain_entity(self, concrete_entity):
        schema = self._get_schema(concrete_entity)
        return schema.get('domain_entity') if schema else None

    def _get_schema(self, concrete_entity):
        spec = self.template.lookup(concrete_entity)
        return spec.get('schema') if spec else None

    def get_concrete_entity_of_tab(self, tab_name):
        tabs_config = self.template.get_tabs_config()
        return tabs_config.get_key_for_label(tab_name)

    def is_identifier_field(self, header_name):
        spec = self.template.lookup(header_name)
        return spec.get('identifiable')

    def get_concrete_entity_of_column(self, header_name):
        match = re.search('(?P<concrete_entity>\w+)(\.\w+)*', header_name)
        concrete_entity = match.group('concrete_entity')
        return concrete_entity


# TODO implement this
def build(schemas) -> TemplateManager: ...


class RowTemplate:

    def __init__(self, cell_conversions, default_values={}):
        self.cell_conversions = cell_conversions
        self.default_values = copy.deepcopy(default_values)

    def do_import(self, row):
        data_node = DataNode(defaults=self.default_values)
        for index, cell in enumerate(row):
            conversion:CellConversion = self.cell_conversions[index]
            conversion.apply(data_node, cell.value)
        return data_node.as_dict()
