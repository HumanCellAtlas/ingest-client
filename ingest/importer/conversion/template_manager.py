from ingest.importer.conversion.data_converter import Converter, ListConverter
from ingest.template.schematemplate import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def get_converter(self, header_name):
        column_spec = self.template.lookup(header_name)
        if (column_spec.get('multivalue')):
            converter = ListConverter()
        else:
            converter = Converter()
        return converter