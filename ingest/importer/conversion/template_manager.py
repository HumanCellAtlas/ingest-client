from ingest.importer.conversion.data_converter import Converter, ListConverter, IntegerConverter
from ingest.template.schematemplate import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def get_converter(self, header_name):
        column_spec = self.template.lookup(header_name)

        if column_spec.get('multivalue'):
            converter = ListConverter()
        else:
            converter = Converter()

        value_type = column_spec.get('value_type')
        if value_type == 'integer':
            converter = IntegerConverter()

        return converter