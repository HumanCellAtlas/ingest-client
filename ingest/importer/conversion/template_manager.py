import re

import ingest.template.schema_template as schema_template

from openpyxl.worksheet import Worksheet

from ingest.importer.conversion.conversion_strategy import CellConversion, DirectCellConversion
from ingest.importer.conversion.data_converter import ListConverter, DataType, CONVERTER_MAP, \
    Converter
from ingest.importer.data_node import DataNode
from ingest.template.schema_template import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def create_template_node(self, worksheet: Worksheet):
        concrete_entity = self.get_concrete_entity_of_tab(worksheet.title)
        schema = self._get_schema(concrete_entity)
        data_node = DataNode()
        data_node['describedBy'] = schema['url']
        data_node['schema_type'] = schema['domain_entity']
        return data_node

    def create_row_template(self, worksheet: Worksheet):
        for row in worksheet.iter_rows(row_offset=3, max_row=1):
            header_row = row
        return RowTemplate([DirectCellConversion(header_row[0].value, Converter())])

    def get_converter(self, header_name):
        column_spec = self.lookup(header_name)

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
        column_spec = self.lookup(parent_field)

        return column_spec and column_spec.get('multivalue') and (
            column_spec.get('value_type') and column_spec.get('value_type') == 'object'
        )

    def _get_parent_field(self, header_name):
        try:
            match = re.search('(?P<field_chain>.*)(\.\w+)', header_name)
            parent_field = match.group('field_chain')
        except:
            raise ParentFieldNotFound(header_name)

        return parent_field

    def get_schema_url(self, concrete_entity):
        schema = self._get_schema(concrete_entity)
        # TODO must query schema endpoint in core to get the latest version
        return schema.get('url') if schema else None

    def get_domain_entity(self, concrete_entity):
        schema = self._get_schema(concrete_entity)
        domain_entity = schema.get('domain_entity') if schema else None

        match = re.search('(?P<domain_entity>\w+)(\/*)', domain_entity)
        domain_entity = match.group('domain_entity')

        return domain_entity

    def _get_schema(self, concrete_entity):
        spec = self.lookup(concrete_entity)
        return spec.get('schema') if spec else None

    def get_concrete_entity_of_tab(self, tab_name):

        try:
            tabs_config = self.template.get_tabs_config()
            concrete_entity = tabs_config.get_key_for_label(tab_name)
        except:
            print(f'No entity found for tab {tab_name}')
            return None

        return concrete_entity

    def is_identifier_field(self, header_name):
        spec = self.lookup(header_name)
        return spec.get('identifiable')

    def get_concrete_entity_of_column(self, header_name):
        match = re.search('(?P<concrete_entity>\w+)(\.\w+)*', header_name)
        concrete_entity = match.group('concrete_entity')
        return concrete_entity

    def get_key_for_label(self, header_name, tab_name):
        key = None

        try:
            key = self.template.get_key_for_label(header_name, tab_name)
        except:
            print(f'{header_name} in "{tab_name}" tab is not found in schema template')

        return key

    def lookup(self, header_name):
        spec = None

        try:
            spec = self.template.lookup(header_name)
        except schema_template.UnknownKeyException:
            print(f'schema_template.UnknownKeyException: Could not lookup {header_name} in template.')
            return {}

        return spec

def build(schemas) -> TemplateManager:
    template = SchemaTemplate(schemas)
    template_mgr = TemplateManager(template)
    return template_mgr


class RowTemplate:

    def __init__(self, cell_conversions, default_values={}):
        self.cell_conversions = cell_conversions
        self.default_values = default_values

    def do_import(self, row):
        data_node = DataNode(defaults=self.default_values)
        for index, cell in enumerate(row):
            conversion:CellConversion = self.cell_conversions[index]
            conversion.apply(data_node, cell.value)
        return data_node.as_dict()


class ParentFieldNotFound(Exception):
    def __init__(self, header_name):
        message = f'{header_name} has no parent field'
        super(ParentFieldNotFound, self).__init__(message)

        self.header_name = header_name
