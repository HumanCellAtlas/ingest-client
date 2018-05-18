from ingest.importer.conversion.data_converter import DataType, CONVERTER_MAP, ListConverter


class ColumnSpecification:

    def __init__(self, field_name, raw_spec, parent=None):
        self.field_name = field_name
        self.data_type = DataType.find(raw_spec.get('value_type'))
        self.multivalue = bool(raw_spec.get('multivalue'))
        if parent is not None:
            self.field_of_list_element = bool(parent.get('multivalue'))

    def is_multivalue(self):
        return self.multivalue

    def is_field_of_list_element(self):
        return self.field_of_list_element

    def determine_converter(self):
        if not self.multivalue:
            default_converter = CONVERTER_MAP.get(DataType.STRING)
            converter = CONVERTER_MAP.get(self.data_type, default_converter)
        else:
            converter = ListConverter(self.data_type)
        return converter

    @staticmethod
    def build(field_name, data_type=DataType.UNDEFINED, multivalue=False):
        raw_spec = {
            'value_type': data_type.value,
            'multivalue': multivalue
        }
        return ColumnSpecification(field_name, raw_spec)