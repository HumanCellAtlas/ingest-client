from ingest.importer.conversion.data_converter import Converter
from ingest.importer.schematemplate import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def get_converter(self, header_name):
        self.template.lookup(header_name)
        return Converter()