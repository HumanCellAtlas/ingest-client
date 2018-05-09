from ingest.importer.conversion.data_converter import ListConverter, DataType, CONVERTER_MAP
from ingest.template.schematemplate import SchemaTemplate


class TemplateManager:

    def __init__(self, template:SchemaTemplate):
        self.template = template

    def get_converter(self, header_name):
        column_spec = self.template.lookup(header_name)
        default_converter = CONVERTER_MAP[DataType.STRING]
        value_type = column_spec.get('value_type')
        data_type = DataType.find(value_type)
        if column_spec.get('multivalue'):
            converter = ListConverter(data_type=data_type)
        else:
            converter = CONVERTER_MAP.get(data_type, default_converter)
        return converter
