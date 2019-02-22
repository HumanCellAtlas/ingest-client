from enum import Enum

from ingest.importer.conversion import utils, data_converter
from ingest.importer.conversion.data_converter import DataType, CONVERTER_MAP, ListConverter
from ingest.template.schema_template import SchemaTemplate


class ConversionType(Enum):
    UNDEFINED = 0,
    MEMBER_FIELD = 1,
    FIELD_OF_LIST_ELEMENT = 2,
    IDENTITY = 3,
    LINKED_IDENTITY = 4,
    EXTERNAL_REFERENCE = 5,
    LINKING_DETAIL = 6


class ColumnSpecification:

    def __init__(self, field_name, object_type, main_category, data_type, multivalue=False,
                 multivalue_parent=False, identity: bool=False, external_reference: bool=False,
                 order_of_occurence: int = 1):
        self.field_name = field_name
        self.object_type = object_type
        self.main_category = main_category
        self.data_type = data_type
        self.multivalue = multivalue
        self.multivalue_parent = multivalue_parent
        self.identity = identity
        self.external_reference = external_reference
        self.order_of_occurence = order_of_occurence

    def is_multivalue(self):
        return self.multivalue

    def is_field_of_list_element(self):
        return self.multivalue_parent

    def is_identity(self):
        return self.identity

    def is_external_reference(self):
        return self.external_reference

    def get_conversion_type(self):
        if self._represents_an_object_field():
            conversion_type = self._determine_conversion_type_for_object_field()
        else:
            conversion_type = self._determine_conversion_type_for_reference_field()
        return conversion_type

    def _represents_an_object_field(self):
        entity_type = utils.extract_root_field(self.field_name)
        return entity_type == self.object_type and self.order_of_occurence == 1

    def _determine_conversion_type_for_object_field(self):
        if self.identity:
            conversion_type = ConversionType.IDENTITY
        elif self.multivalue_parent:
            conversion_type = ConversionType.FIELD_OF_LIST_ELEMENT
        else:
            conversion_type = ConversionType.MEMBER_FIELD
        return conversion_type

    def _determine_conversion_type_for_reference_field(self):
        if self.identity:
            if self.external_reference:
                conversion_type = ConversionType.EXTERNAL_REFERENCE
            else:
                conversion_type = ConversionType.LINKED_IDENTITY
        else:
            conversion_type = ConversionType.LINKING_DETAIL
        return conversion_type

    def determine_converter(self):
        if not self.multivalue:
            converter = CONVERTER_MAP.get(self.data_type, data_converter.DEFAULT)
        else:
            converter = ListConverter(self.data_type)
        return converter

    @staticmethod
    def build_raw(field_name, object_type, main_category, raw_spec, order_of_occurence=1, parent=None):
        data_type = DataType.find(raw_spec.get('value_type'))
        multivalue = bool(raw_spec.get('multivalue'))
        multivalue_parent = bool(parent.get('multivalue')) if parent != None else False
        identity: bool = bool(raw_spec.get('identifiable'))
        external_reference = bool(raw_spec.get('external_reference'))
        return ColumnSpecification(field_name, object_type, main_category, data_type,
                                   multivalue=multivalue, multivalue_parent=multivalue_parent,
                                   identity=identity, external_reference=external_reference,
                                   order_of_occurence=order_of_occurence)


# TODO this should be in the SchemaTemplate class
def look_up(schema_template: SchemaTemplate, header, context=None):
    # Context refers to the context by which the header is being specified for.
    # For example, the property `project.contributors.email` will have a slightly different
    # specification in the context of `project.contributors`, than in the context of `project`.
    # In the former, email does not have a multivalue parent, whereas it has in the latter.
    # Framing it differently, in the former, it is the `contributors` that's being defined; in
    # the latter it is the `project` that's being defined.
    concrete_type = utils.extract_root_field(header)
    if not context:
        context = concrete_type

    type_spec = schema_template.lookup(concrete_type)
    schema = type_spec.get('schema')
    domain_type, *_ = schema.get('domain_entity').split('/')

    parent_field, *_ = utils.split_field_chain(header)
    parent_spec = schema_template.lookup(parent_field) if parent_field != context else None

    field_spec = schema_template.lookup(header)
    data_type = field_spec.get('value_type')
    return ColumnSpecification(header, concrete_type, domain_type, data_type,
                               identity=field_spec.get('identifiable'),
                               external_reference=field_spec.get('external_reference'),
                               multivalue=field_spec.get('multivalue'),
                               multivalue_parent=(parent_spec and parent_spec.get('multivalue')))
