import unittest

from ingest.template.descriptor import ComplexPropertyDescriptor, SchemaTypeDescriptor, SimplePropertyDescriptor


class TestDescriptor(unittest.TestCase):
    """ Testing class for the Descriptor class. """

    def test_custom_location_support(self):
        sample_metadata_schema_url = "https://humancellatlas.org/schema/type/protocol/sequencing/10.1.0" \
                                     "/sequencing_protocol"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "protocol/sequencing",
                                              "module": "sequencing_protocol", "version": "10.1.0",
                                              "url": sample_metadata_schema_url}
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(),
                         expected_dictionary_representation)

    def test_domain_entity_containing_slash_support(self):
        sample_metadata_schema_url = "https://schema.humancellatlas.org/type/protocol/sequencing/10.1.0" \
                                     "/sequencing_protocol"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "protocol/sequencing",
                                              "module": "sequencing_protocol", "version": "10.1.0",
                                              "url": sample_metadata_schema_url}
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(),
                         expected_dictionary_representation)

    def test_version_entity_has_major(self):
        sample_metadata_schema_url = "https://schema.humancellatlas.org/type/protocol/sequencing/10/sequencing_protocol"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "protocol/sequencing",
                                              "module": "sequencing_protocol", "version": "10",
                                              "url": sample_metadata_schema_url}
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(),
                         expected_dictionary_representation)

    def test_version_entity_has_major_minor(self):
        sample_metadata_schema_url = "https://schema.humancellatlas.org/type/protocol/sequencing/10.1" \
                                     "/sequencing_protocol"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "protocol/sequencing",
                                              "module": "sequencing_protocol", "version": "10.1",
                                              "url": sample_metadata_schema_url}
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(),
                         expected_dictionary_representation)

    def test_version_entity_has_major_minor_revision(self):
        sample_metadata_schema_url = "https://schema.humancellatlas.org/type/protocol/sequencing/10.1.5" \
                                     "/sequencing_protocol"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "protocol/sequencing",
                                              "module": "sequencing_protocol", "version": "10.1.5",
                                              "url": sample_metadata_schema_url}
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(),
                         expected_dictionary_representation)

    def test__schema_type_descriptor__success(self):
        sample_metadata_schema_url = "https://schema.humancellatlas.org/type/biomaterial/10.0.2/organoid"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "biomaterial",
                                              "module": "organoid", "version": "10.0.2",
                                              "url": sample_metadata_schema_url}
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(),
                         expected_dictionary_representation)

    def test__schema_type_descriptor_with_missing_components__throws_exception(self):
        sample_metadata_schema_url = "https://schema.humancellatlas.org/type/biomaterial/organoid"

        with self.assertRaisesRegex(Exception, "does not conform to expected format"):
            SchemaTypeDescriptor(sample_metadata_schema_url)

    def test__simple_property_descriptor__success(self):
        sample_simple_property_description = "The version number of the schema in major.minor.patch format."
        sample_simple_property_type = "string"
        sample_simple_property_pattern = "^[0-9]{1,}.[0-9]{1,}.[0-9]{1,}$"
        sample_simple_property_example = "4.6.1"
        sample_simple_metadata_schema_json = {

            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "pattern": sample_simple_property_pattern,
            "example": sample_simple_property_example
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_type,
            "example": sample_simple_property_example, "multivalue": False, "external_reference": False,
            "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__simple_array_property_descriptor__success(self):
        sample_simple_property_description = "The version number of the schema in major.minor.patch format."
        sample_simple_property_type = "array"
        sample_simple_property_pattern = "^[0-9]{1,}.[0-9]{1,}.[0-9]{1,}$"
        sample_simple_property_example = "4.6.1"
        sample_simple_property_array_type = "string"
        sample_simple_metadata_schema_json = {
            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "pattern": sample_simple_property_pattern,
            "example": sample_simple_property_example,
            "items": {
                "type": sample_simple_property_array_type
            }
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_array_type,
            "example": sample_simple_property_example, "multivalue": True, "external_reference": False,
            "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__complex_property_description__success(self):
        top_level_metadata_schema_url = "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse"
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": top_level_metadata_schema_url,
            "description": "Information relating to a timecourse.",
            "required": [
                "unit"
            ],
            "type": "object",
            "properties": {
                "value": {
                    "description": "The numerical value in Timecourse unit",
                    "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                    "type": "string",
                    "example": "2; 5.5-10.5",
                    "user_friendly": "Timecourse value",
                    "guidelines": "Enter either a single value or a range of values. Indicate a range using a hyphen."
                },
                "unit": {
                    "description": "The unit in which the Timecourse value is expressed.",
                    "type": "object",
                    "user_friendly": "Timecourse unit"
                },
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        expected_top_level_schema_descriptor = {
            "high_level_entity": "module", "domain_entity": "biomaterial", "module": "timecourse", "version": "2.0.2",
            "url": top_level_metadata_schema_url
        }
        expected_top_level_schema_properties = {
            "description": "Information relating to a timecourse.", "value_type": "object", "multivalue": False,
            "external_reference": False, "required": False, "identifiable": False
        }
        expected_child_property_value_descriptor = {
            "description": "The numerical value in Timecourse unit", "value_type": "string", "example": "2; 5.5-10.5",
            "multivalue": False, "external_reference": False, "user_friendly": "Timecourse value",
            "guidelines": "Enter either a single value or a range of values. Indicate a range using a hyphen.",
            "required": False, "identifiable": False
        }
        expected_child_property_unit_descriptor = {
            "description": "The unit in which the Timecourse value is expressed.", "value_type": "object",
            "multivalue": False, "external_reference": False, "user_friendly": "Timecourse unit", "required": True,
            "identifiable": False
        }

        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor, "value": expected_child_property_value_descriptor,
            "unit": expected_child_property_unit_descriptor, "required_properties": ["unit"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__complex_property_description_with_embedded_schema__success(self):
        top_level_metadata_schema_url = "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse"
        embedded_metadata_schema_url = "https://schema.humancellatlas.org/module/ontology/5.3.5/time_unit_ontology"
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": top_level_metadata_schema_url,
            "description": "Information relating to a timecourse.",
            "required": [
                "unit"
            ],
            "type": "object",
            "properties": {
                "value": {
                    "description": "The numerical value in Timecourse unit",
                    "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                    "type": "string",
                    "example": "2; 5.5-10.5",
                },
                "unit": {
                    "description": "The unit in which the Timecourse value is expressed.",
                    "type": "object",
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "$id": embedded_metadata_schema_url,
                    "properties": {
                        "ontology": {
                            "description": "An ontology term identifier in the form prefix:accession.",
                            "type": "string",
                            "graph_restriction": {
                                "ontologies": [
                                    "obo:efo",
                                    "obo:uo"
                                ],
                                "classes": [
                                    "UO:0000003",
                                    "UO:0000149"
                                ],
                                "relations": [
                                    "rdfs:subClassOf"
                                ],
                                "direct": False,
                                "include_self": False
                            },
                            "example": "UO:0000010; UO:0000034",
                            "user_friendly": "Time unit ontology ID"
                        },
                        "ontology_label": {
                            "description": "The preferred label for the ontology term",
                            "type": "string",
                            "example": "second; week",
                            "user_friendly": "Time unit ontology label"
                        }
                    },
                    "user_friendly": "Timecourse unit"
                },
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        # Create the expected dictionary representation of the embedded schema property.
        expected_embedded_schema_descriptor = {
            "high_level_entity": "module", "domain_entity": "ontology", "module": "time_unit_ontology",
            "version": "5.3.5", "url": embedded_metadata_schema_url
        }
        expected_embedded_schema_properties = {
            "description": "The unit in which the Timecourse value is expressed.", "value_type": "object",
            "multivalue": False, "external_reference": False, "user_friendly": "Timecourse unit", "required": True,
            "identifiable": False
        }
        expected_child_property_ontology_descriptor = {
            "description": "An ontology term identifier in the form prefix:accession.", "value_type": "string",
            "example": "UO:0000010; UO:0000034", "multivalue": False, "external_reference": False,
            "user_friendly": "Time unit ontology ID", "required": False, "identifiable": False
        }
        expected_child_property_ontology_label_descriptor = {
            "description": "The preferred label for the ontology term", "value_type": "string",
            "example": "second; week", "multivalue": False, "external_reference": False,
            "user_friendly": "Time unit ontology label", "required": False, "identifiable": False
        }
        expected_child_property_unit_descriptor = {
            "schema": expected_embedded_schema_descriptor, "ontology": expected_child_property_ontology_descriptor,
            "ontology_label": expected_child_property_ontology_label_descriptor
        }
        expected_child_property_unit_descriptor.update(expected_embedded_schema_properties)

        # Create the top level expected dictionary representation
        expected_top_level_schema_descriptor = {
            "high_level_entity": "module", "domain_entity": "biomaterial", "module": "timecourse", "version": "2.0.2",
            "url": top_level_metadata_schema_url
        }
        expected_top_level_schema_properties = {
            "description": "Information relating to a timecourse.", "value_type": "object", "multivalue": False,
            "external_reference": False, "required": False, "identifiable": False
        }
        expected_child_property_value_descriptor = {
            "description": "The numerical value in Timecourse unit", "value_type": "string", "example": "2; 5.5-10.5",
            "multivalue": False, "external_reference": False, "required": False, "identifiable": False
        }
        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor, "value": expected_child_property_value_descriptor,
            "unit": expected_child_property_unit_descriptor, "required_properties": ["unit"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__complex_property_description_with_multivalue_embedded_schema__success(self):
        top_level_metadata_schema_url = "https://schema.humancellatlas.org/module/biomaterial/2.0.2/some_schema"
        embedded_metadata_schema_url = "https://schema.humancellatlas.org/module/biomaterial/5.3.5/some_embedded_schema"
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": top_level_metadata_schema_url,
            "description": "Information relating to a schema.",
            "required": [
                "some_multivalue_property"
            ],
            "type": "object",
            "properties": {
                "some_multivalue_property": {
                    "description": "A multivalue property",
                    "type": "array",
                    "items": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "$id": embedded_metadata_schema_url,
                        "description": "A multivalue property",
                        "type": "object",
                        "properties": {
                            "some_embedded_schema_property": {
                                "description": "Some embedded schema property",
                                "type": "string",
                            }
                        }
                    }
                },
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        # Create the expected dictionary representation of the embedded schema property.
        expected_embedded_schema_descriptor = {
            "high_level_entity": "module", "domain_entity": "biomaterial", "module": "some_embedded_schema",
            "version": "5.3.5", "url": embedded_metadata_schema_url
        }
        expected_embedded_schema_properties = {
            "description": "A multivalue property", "value_type": "object", "multivalue": True,
            "external_reference": False, "required": True, "identifiable": False
        }
        expected_embedded_child_property_descriptor = {
            "description": "Some embedded schema property", "value_type": "string", "multivalue": False,
            "external_reference": False, "required": False, "identifiable": False
        }
        expected_child_property_descriptor = {
            "schema": expected_embedded_schema_descriptor,
            "some_embedded_schema_property": expected_embedded_child_property_descriptor
        }
        expected_child_property_descriptor.update(expected_embedded_schema_properties)

        # Create the top level expected dictionary representation
        expected_top_level_schema_descriptor = {
            "high_level_entity": "module", "domain_entity": "biomaterial", "module": "some_schema", "version": "2.0.2",
            "url": top_level_metadata_schema_url
        }
        expected_top_level_schema_properties = {
            "description": "Information relating to a schema.", "value_type": "object", "multivalue": False,
            "external_reference": False, "required": False, "identifiable": False
        }
        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor,
            "some_multivalue_property": expected_child_property_descriptor,
            "required_properties": ["some_multivalue_property"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)

        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__complex_identifiable_property_descriptor__success(self):
        top_level_metadata_schema_url = "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse"
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": top_level_metadata_schema_url,
            "description": "Information relating to a timecourse.",
            "required": [
                "value"
            ],
            "type": "object",
            "properties": {
                "protocol_id": {
                    "description": "A random id.",
                    "type": "string",
                    "user_friendly": "Protocol Id"
                },
                "value": {
                    "description": "The numerical value in Timecourse unit",
                    "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                    "type": "string",
                    "example": "2; 5.5-10.5",
                    "user_friendly": "Timecourse value",
                    "guidelines": "Enter either a single value or a range of values. Indicate a range using a hyphen."
                }
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        expected_top_level_schema_descriptor = {
            "high_level_entity": "module", "domain_entity": "biomaterial", "module": "timecourse", "version": "2.0.2",
            "url": top_level_metadata_schema_url
        }
        expected_top_level_schema_properties = {
            "description": "Information relating to a timecourse.", "value_type": "object", "multivalue": False,
            "external_reference": False, "required": False, "identifiable": False
        }
        expected_child_property_id_descriptor = {
            "description": "A random id.", "value_type": "string",
            "multivalue": False, "external_reference": False, "user_friendly": "Protocol Id", "required": False,
            "identifiable": True
        }
        expected_child_property_value_descriptor = {
            "description": "The numerical value in Timecourse unit", "value_type": "string", "example": "2; 5.5-10.5",
            "multivalue": False, "external_reference": False, "user_friendly": "Timecourse value",
            "guidelines": "Enter either a single value or a range of values. Indicate a range using a hyphen.",
            "required": True, "identifiable": False
        }
        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor, "protocol_id": expected_child_property_id_descriptor,
            "value": expected_child_property_value_descriptor, "required_properties": ["value"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)
