import re

from openpyxl.worksheet import Worksheet

from ingest.importer.conversion.conversion_strategy import CellConversion
from ingest.importer.conversion.data_converter import ListConverter, DataType, CONVERTER_MAP
from ingest.importer.data_node import DataNode
from ingest.template.schematemplate import SchemaTemplate


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
        return RowTemplate([CellConversion(header_row[0].value, None)])

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

    def get_schema_type(self, concrete_entity):
        schema = self._get_schema(concrete_entity)
        return schema.get('domain_entity') if schema else None

    def _get_schema(self, concrete_entity):
        spec = self.template.lookup(concrete_entity)
        return spec.get('schema') if spec else None


# TODO implement this
def build(schemas) -> TemplateManager: ...


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
