import unittest

from ingest.template.descriptor import ComplexPropertyDescriptor
from ingest.template.schema_parser import SchemaParser


class TestSchemaParser(unittest.TestCase):
    """ Testing class for the SchemaParser class. """

    def test__removed_ignored_properties_from_descriptor__success(self):
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
        sample_ignored_properties = ["description"]

        schema_parser = SchemaParser(sample_complex_metadata_schema_json, sample_ignored_properties)

        expected_descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)
        self.assertEqual(expected_descriptor.get_dictionary_representation_of_descriptor(),
                         schema_parser.schema_descriptor.get_dictionary_representation_of_descriptor())

        # First, check that the ignored property existed when parsing the schema.
        [self.assertIn(ignored_property,
                       schema_parser.schema_descriptor.get_dictionary_representation_of_descriptor().keys())
         for ignored_property in sample_ignored_properties]

        # Second, check that the ignored properties are removed once the post processing step has completed.
        [self.assertNotIn(ignored_property, schema_parser.schema_dictionary.keys())
         for ignored_property in sample_ignored_properties]

    def test__descriptor_with_no_ignored_properties__unchanged(self):
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
        sample_ignored_properties = []

        schema_parser = SchemaParser(sample_complex_metadata_schema_json, sample_ignored_properties)

        expected_descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)
        self.assertEqual(expected_descriptor.get_dictionary_representation_of_descriptor(),
                         schema_parser.schema_descriptor.get_dictionary_representation_of_descriptor())

        # Check to make sure that the initially created Descriptor and the post-processed descriptor are exactly the
        # same.
        self.assertEqual(schema_parser.schema_descriptor.get_dictionary_representation_of_descriptor(),
                         schema_parser.schema_dictionary)

    def test__get_maps_of_simple_property_paths__success(self):
        sample_simple_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
        sample_ignored_properties = []

        schema_parser = SchemaParser(sample_simple_metadata_schema_json, sample_ignored_properties)
        actual_label_map = schema_parser.get_map_of_paths_by_property_label(
            {"timecourse": schema_parser.schema_dictionary})

        expected_label_map = {
            "timecourse value": ["timecourse.value"],
            "timecourse unit": ["timecourse.unit"],
            "timecourse.value": ["timecourse.value"],
            "timecourse.unit": ["timecourse.unit"]
        }
        self.assertEqual(expected_label_map, actual_label_map)

    def test__get_maps_of_deep_property_paths__success(self):
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
                    "$id": "https://schema.humancellatlas.org/module/ontology/5.3.5/time_unit_ontology",
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
        sample_ignored_properties = []

        schema_parser = SchemaParser(sample_complex_metadata_schema_json, sample_ignored_properties)
        actual_label_map = schema_parser.get_map_of_paths_by_property_label(
            {"timecourse": schema_parser.schema_dictionary})

        expected_label_map = {
            "timecourse unit": ["timecourse.unit"],
            "timecourse.value": ["timecourse.value"],
            "timecourse.unit": ["timecourse.unit"],
            "timecourse.unit.ontology": ["timecourse.unit.ontology"],
            "time unit ontology id": ["timecourse.unit.ontology"],
            "timecourse.unit.ontology_label": ["timecourse.unit.ontology_label"],
            "time unit ontology label": ["timecourse.unit.ontology_label"]
        }
        self.assertEqual(expected_label_map, actual_label_map)

    def test__get_maps_of_duplicated_property_paths__success(self):
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
                    "$id": "https://schema.humancellatlas.org/module/ontology/5.3.5/time_unit_ontology",
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
                            "user_friendly": "Timecourse unit"
                        }
                    },
                    "user_friendly": "Timecourse unit"
                },
            }
        }
        sample_ignored_properties = []

        schema_parser = SchemaParser(sample_complex_metadata_schema_json, sample_ignored_properties)
        actual_label_map = schema_parser.get_map_of_paths_by_property_label(
            {"timecourse": schema_parser.schema_dictionary})

        expected_label_map = {
            "timecourse unit": ["timecourse.unit", "timecourse.unit.ontology_label"],
            "timecourse.value": ["timecourse.value"],
            "timecourse.unit": ["timecourse.unit"],
            "timecourse.unit.ontology": ["timecourse.unit.ontology"],
            "time unit ontology id": ["timecourse.unit.ontology"],
            "timecourse.unit.ontology_label": ["timecourse.unit.ontology_label"],
        }
        self.assertEqual(expected_label_map, actual_label_map)

    def test__get_tab_presentation_of_simple_metadata_schema__success(self):
        sample_simple_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
        sample_ignored_properties = []

        schema_parser = SchemaParser(sample_simple_metadata_schema_json, sample_ignored_properties)
        actual_tab_representation = schema_parser.get_tab_representation_of_schema()

        expected_tab_representation = {"timecourse": {"display_name": "Timecourse",
                                                      "columns": ["timecourse.value", "timecourse.unit"]}}
        self.assertEqual(expected_tab_representation, actual_tab_representation)

    def test__get_tab_presentation_of_complex_metadata_schema__success(self):
        sample_complex_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
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
                    "$id": "https://schema.humancellatlas.org/module/ontology/5.3.5/time_unit_ontology",
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
                            "user_friendly": "Timecourse unit"
                        }
                    },
                    "user_friendly": "Timecourse unit"
                },
            }
        }
        sample_ignored_properties = []

        schema_parser = SchemaParser(sample_complex_metadata_schema_json, sample_ignored_properties)
        actual_tab_representation = schema_parser.get_tab_representation_of_schema()

        expected_tab_representation = {"timecourse": {"display_name": "Timecourse",
                                                      "columns": ["timecourse.value",
                                                                  "timecourse.unit.ontology",
                                                                  "timecourse.unit.ontology_label"]}}
        self.assertEqual(expected_tab_representation, actual_tab_representation)
