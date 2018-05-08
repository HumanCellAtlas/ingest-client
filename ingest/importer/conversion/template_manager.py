from ingest.importer.schematemplate import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def get_converter(self, header_name):
        return {}