from ingest.importer.conversion.data_converter import ListConverter, DataType, CONVERTER_MAP
from ingest.template.schematemplate import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def get_converter(self, header_name):
        column_spec = self.template.lookup(header_name)
        default_converter = CONVERTER_MAP[DataType.STRING]
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

    # TODO implement this
    def is_ontology_subfield(self, field): ...

    # TODO implement this
    def get_schema_url(self): ...

    # TODO implement this
    def get_schema_type(self): ...
