from unittest import TestCase
from unittest.mock import Mock, patch

from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import BooleanConverter, DataType, DefaultConverter, IntegerConverter, \
    ListConverter, StringConverter
from ingest.template.schema_template import SchemaTemplate


class ColumnSpecificationTest(TestCase):

    def setUp(self):
        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/somedomain/2.0.2/someschema",
            "description": "Just a plain old test schema",
            "required": [
                "required_property"
            ],
            "type": "object",
            "properties": {
                "protocol_id": {
                    "type": "integer",
                    "multivalue": False,
                },
                "value": {
                    "description": "The numerical value in Timecourse unit",
                    "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                    "type": "string",
                    "example": "2; 5.5-10.5",
                    "user_friendly": "Timecourse value",
                    "guidelines": "Enter either a single value or a range of values. Indicate a range using a hyphen."
                },
                "required_property": {
                    "description": "Some generic required property",
                    "type": "object",
                    "user_friendly": "Required property"
                },
                "multivalue_property": {
                    "description": "Some generic multivalue property",
                    "type": "array",
                    "multivalue": True,
                    "items": {
                        "type": "integer"
                    }
                },
                "module_property": {
                    'description': 'The scientific binomial name for the species of the organism.',
                    'type': 'array',
                    'items': {
                        '$schema': 'http://json-schema.org/draft-07/schema#',
                        '$id': 'https://schema.humancellatlas.org/module/ontology/5.3.5/species_ontology',
                        'description': 'A term that may be associated with a species-related ontology term.',
                        'additionalProperties': False,
                        'required': ['text'],
                        'title': 'Species ontology',
                        'name': 'species_ontology',
                        'type': 'object',
                        'properties': {
                            'describedBy': {
                                'description': 'The URL reference to the schema.',
                                'type': 'string',
                                'pattern': '^(http|https)://schema.(.*?)humancellatlas.org/module/ontology/(([0-9]{1,'
                                           '}.[0-9]{1,}.[0-9]{1,})|([a-zA-Z]*?))/species_ontology'
                            },
                            'schema_version': {
                                'description': 'The version number of the schema in major.minor.patch format.',
                                'type': 'string',
                                'pattern': '^[0-9]{1,}.[0-9]{1,}.[0-9]{1,}$',
                                'example': '4.6.1'
                            },
                            'text': {
                                'description': 'The name of the species to which the organism belongs.',
                                'type': 'string',
                                'example': 'Hom...'
                            },
                            'ontology': {
                                'description': 'An ontology term identifier in the form prefix:accession.',
                                'type': 'string',
                                'graph_restriction': {
                                    'ontologies': ['obo:efo', 'obo:NCBITaxon'],
                                    'classes': ['OBI:0100026', 'NCBITaxon:2759'],
                                    'relations': ['rdfs:subClassOf'],
                                    'direct': False,
                                    'include_self': False
                                },
                                'example': 'NCBITaxon:9606; NCBITaxon:10090',
                                'user_friendly': 'Species ontology ID'
                            },
                            'ontology_label': {
                                'description': 'The preferred label for the ontology term referred to in the ontology '
                                               'field. This may differ from the user-supplied value in the text field.',
                                'type': 'string',
                                'example': 'Homo sapiens; Mus musculus',
                                'user_friendly': 'Species ontology label'
                            }
                        }
                    }
                }
            }
        }
        self._mock_fetching_of_property_migrations()

        self.schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

    def test__column_specification_creation_identifiable__succeeds(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.protocol_id", "someschema")

        self.assertTrue(column_specification.is_identity())
        self.assertEqual('someschema', column_specification.context_concrete_type)
        self.assertEqual('somedomain', column_specification.domain_type)
        self.assertEqual('someschema.protocol_id', column_specification.field_name)
        self.assertEqual(DataType.INTEGER, column_specification.data_type)
        self.assertEqual(ConversionType.IDENTITY, column_specification.get_conversion_type())

    def test__column_specification_creation_string_type__succeeds(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.value", "someschema",
                                                   order_of_occurrence=7)

        self.assertFalse(column_specification.multivalue)
        self.assertEqual('someschema', column_specification.context_concrete_type)
        self.assertEqual('somedomain', column_specification.domain_type)
        self.assertEqual('someschema.value', column_specification.field_name)
        self.assertEqual(DataType.STRING, column_specification.data_type)
        self.assertEqual(7, column_specification.order_of_occurrence)

    def test_look_up_linked_object_field(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.value", "some_linked_field")

        self.assertEqual('somedomain', column_specification.domain_type)
        self.assertEqual('some_linked_field', column_specification.context_concrete_type)
        self.assertEqual(ConversionType.LINKING_DETAIL, column_specification.get_conversion_type())

    def test_look_up_nested_object_field(self):
        nested_sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/somedomain/2.0.2/someschema",
            "description": "Just a plain old test schema",
            "required": [],
            "type": "object",
            "properties": {
                "some_parent_property": {
                    "description": "A parent property",
                    "type": "array",
                    "multivalue": True,
                    "items": {
                        "type": "integer"
                    },
                    "properties": {
                        "some_child_property": {
                            "description": "A child property",
                            "type": "string",
                        }
                    }
                }
            }
        }
        schema_template = SchemaTemplate(json_schema_docs=[nested_sample_metadata_schema_json])

        column_specification = ColumnSpecification(schema_template,
                                                   "someschema.some_parent_property.some_child_property", "someschema")

        self.assertFalse(column_specification.multivalue)
        self.assertTrue(column_specification.is_field_of_list_element())
        self.assertEqual(ConversionType.FIELD_OF_LIST_ELEMENT, column_specification.get_conversion_type())

    def test_determine_converter_for_single_value(self):
        data_types_to_test = [DataType.BOOLEAN, DataType.INTEGER, DataType.STRING, DataType.UNDEFINED]
        expected_respective_converter = [BooleanConverter, IntegerConverter, StringConverter, DefaultConverter]

        for data_type, expected_converter in zip(data_types_to_test, expected_respective_converter):
            sample_metadata_schema_json = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": "https://schema.humancellatlas.org/module/protocol/2.0.2/someschema",
                "description": "Just a plain old test schema",
                "type": "object",
                "properties": {
                    "some_property": {
                        "description": "Some generic property",
                        "type": data_type.value,
                        "multivalue": False,
                    }
                }
            }
            schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

            column_specification = ColumnSpecification(schema_template, "someschema.some_property", "someschema")

            self.assertIsInstance(column_specification.determine_converter(), expected_converter)
            self.assertEqual(column_specification.get_conversion_type(), ConversionType.MEMBER_FIELD)

    def test_determine_converter_for_multivalue_type(self):
        data_types_to_test = [DataType.BOOLEAN, DataType.INTEGER, DataType.STRING, DataType.UNDEFINED]

        for data_type in data_types_to_test:
            sample_metadata_schema_json = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": "https://schema.humancellatlas.org/module/protocol/2.0.2/someschema",
                "description": "Just a plain old test schema",
                "type": "object",
                "properties": {
                    "multivalue_property": {
                        "description": "Some generic multivalue property",
                        "type": "array",
                        "multivalue": True,
                        "items": {
                            "type": data_type.value
                        }
                    }
                }
            }
            schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

            column_specification = ColumnSpecification(schema_template, "someschema.multivalue_property", "someschema")

            self.assertEqual(column_specification.field_name, "someschema.multivalue_property")
            self.assertTrue(column_specification.is_multivalue())
            self.assertIsInstance(column_specification.determine_converter(), ListConverter)
            self.assertEqual(column_specification.determine_converter().base_type, data_type)

    def test_get_conversion_type_member_field(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.value", "someschema")

        self.assertEqual(ConversionType.MEMBER_FIELD,
                         column_specification.get_conversion_type())

    def test_get_conversion_type_field_of_list(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.module_property.ontology",
                                                   "someschema")

        self.assertEqual(ConversionType.FIELD_OF_LIST_ELEMENT,
                         column_specification.get_conversion_type())

    def test_get_conversion_type_linked_identity(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.protocol_id", "someotherschema")

        self.assertEqual(ConversionType.LINKED_IDENTITY,
                         column_specification.get_conversion_type())

    def test_get_conversion_type_linked_external_reference(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.uuid", "someotherschema")

        self.assertEqual(ConversionType.LINKED_EXTERNAL_REFERENCE,
                         column_specification.get_conversion_type())

    def test_get_conversion_type_linking_detail(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.linking_detail",
                                                   "someotherschema")

        self.assertEqual(ConversionType.LINKING_DETAIL, column_specification.get_conversion_type())

    def test_get_conversion_type_external_reference(self):
        column_specification = ColumnSpecification(self.schema_template, "someschema.uuid",
                                                   "someschema")

        self.assertEqual(ConversionType.EXTERNAL_REFERENCE, column_specification.get_conversion_type())

    @patch("requests.get")
    def _mock_fetching_of_property_migrations(self, property_migrations_request_mock):
        property_migrations_request_mock.return_value = Mock(ok=True)
        property_migrations_request_mock.return_value.json.return_value = {'migrations': []}
