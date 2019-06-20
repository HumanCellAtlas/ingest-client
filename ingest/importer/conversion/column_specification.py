import logging
from enum import Enum

from ingest.importer.conversion import utils, data_converter
from ingest.importer.conversion.data_converter import DataType, CONVERTER_MAP, ListConverter
from ingest.template.schema_template import SchemaTemplate, UnknownKeyException

UNKNOWN_DOMAIN_TYPE = '_unknown_type'

_LOGGER = logging.getLogger(__name__)


class ConversionType(Enum):
    UNDEFINED = 0,
    MEMBER_FIELD = 1,
    FIELD_OF_LIST_ELEMENT = 2,
    IDENTITY = 3,
    LINKED_IDENTITY = 4,
    LINKED_EXTERNAL_REFERENCE = 5,
    LINKING_DETAIL = 6,
    EXTERNAL_REFERENCE = 7


class ColumnSpecification:

    # context_concrete_type is the concrete type of the Metadata Entity for which the column
    # represented by this object is specified for.
    def __init__(self, field_name, context_concrete_type, domain_type, data_type,
                 multivalue=False, multivalue_parent=False, identity: bool = False,
                 external_reference: bool = False, order_of_occurrence: int = 1):
        self.field_name = field_name
        self.context_concrete_type = context_concrete_type
        self.domain_type = domain_type
        self.data_type = data_type
        self.multivalue = multivalue
        self.multivalue_parent = multivalue_parent
        self.identity = identity
        self.external_reference = external_reference
        self.order_of_occurrence = order_of_occurrence
        self.entity_type = utils.extract_root_field(field_name)

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
        return self.entity_type == self.context_concrete_type and self.order_of_occurrence == 1 and not \
            self.external_reference

    def _determine_conversion_type_for_object_field(self):
        if self.identity:
            conversion_type = ConversionType.IDENTITY
        elif self.multivalue_parent:
            conversion_type = ConversionType.FIELD_OF_LIST_ELEMENT
        else:
            conversion_type = ConversionType.MEMBER_FIELD
        return conversion_type

    def _determine_conversion_type_for_reference_field(self):
        if self.external_reference and self.entity_type == self.context_concrete_type:
            conversion_type = ConversionType.EXTERNAL_REFERENCE
        elif self.external_reference and self.entity_type != self.context_concrete_type:
            conversion_type = ConversionType.LINKED_EXTERNAL_REFERENCE
        elif self.identity:
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


# TODO this should be in the SchemaTemplate class
def look_up(schema_template: SchemaTemplate, header, context_concrete_type, context=None,
            order_of_occurrence=1):
    # Context refers to the context in which the header is being specified for.
    # For example, the property `project.contributors.email` will have a slightly different
    # specification in the context of `project.contributors`, than in the context of `project`.
    # In the former, email does not have a multivalue parent, whereas it has in the latter.
    # Framing it differently, in the former, it is each of the `contributors` that's being defined;
    # in the latter it is the `project` that's being defined.
    if not context:
        context = context_concrete_type

    parent_field, *_ = utils.split_field_chain(header)
    parent_spec = _map_key_to_spec(schema_template, parent_field) \
        if parent_field != context else None

    field_spec = _map_key_to_spec(schema_template, header)
    data_type = DataType.find(field_spec.get('value_type'))

    # concrete_type is the actual concrete type that the header represents. Particularly in cases
    # where the column represents a linking detail to another type, concrete_type is different from
    # context_concrete_type. concrete_type is the "inherent" type of the column whichever context
    # it is specified in.
    concrete_type = utils.extract_root_field(header)
    type_spec = _map_key_to_spec(schema_template, concrete_type)
    domain_type = UNKNOWN_DOMAIN_TYPE
    if type_spec:
        schema = type_spec.get('schema')
        domain_type, *_ = schema.get('domain_entity').split('/')

    return ColumnSpecification(header, context_concrete_type, domain_type, data_type,
                               identity=field_spec.get('identifiable'),
                               external_reference=field_spec.get('external_reference'),
                               multivalue=field_spec.get('multivalue'),
                               multivalue_parent=(parent_spec and parent_spec.get('multivalue')),
                               order_of_occurrence=order_of_occurrence)


def _map_key_to_spec(schema_template: SchemaTemplate, key):
    try:
        spec = schema_template.lookup(key)
    except UnknownKeyException as key_exception:
        _LOGGER.warning(f'[{key}] not found in the schema.')
        spec = {}
    return spec
