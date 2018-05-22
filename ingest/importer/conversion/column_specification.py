from enum import Enum

from ingest.importer.conversion.data_converter import DataType, CONVERTER_MAP, ListConverter


class ConversionType(Enum):
    MEMBER_FIELD = 1


class ColumnSpecification:

    def __init__(self, field_name, data_type, multivalue=False, multivalue_parent=False,
                 identity: bool=False):
        self.field_name = field_name
        self.data_type = data_type
        self.multivalue = multivalue
        self.multivalue_parent = multivalue_parent
        self.identity = identity

    def is_multivalue(self):
        return self.multivalue

    def is_field_of_list_element(self):
        return self.multivalue_parent

    def is_identity(self):
        return self.identity

    def get_conversion_type(self):
        return ConversionType.MEMBER_FIELD

    def determine_converter(self):
        if not self.multivalue:
            default_converter = CONVERTER_MAP.get(DataType.STRING)
            converter = CONVERTER_MAP.get(self.data_type, default_converter)
        else:
            converter = ListConverter(self.data_type)
        return converter

    @staticmethod
    def build_raw(field_name, raw_spec, parent=None):
        data_type = DataType.find(raw_spec.get('value_type'))
        multivalue = bool(raw_spec.get('multivalue'))
        multivalue_parent = bool(parent.get('multivalue')) if parent != None else False
        identity: bool = bool(raw_spec.get('identifiable'))
        return ColumnSpecification(field_name, data_type, multivalue=multivalue,
                                   multivalue_parent=multivalue_parent, identity=identity)