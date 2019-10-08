import unittest
from unittest.mock import patch, Mock

import yaml
from sortedcontainers import SortedDict

from ingest.template.descriptor import SimplePropertyDescriptor
from ingest.template.exceptions import UnknownKeySchemaException
from ingest.template.schema_parser import SchemaParser
from ingest.template.schema_template import SchemaTemplate


class TestSchemaTemplate(unittest.TestCase):
    """ Testing class for the SchemaTemplateclass. """

    def test__creation_of_template_with_urls_and_jsons__throws_exception(self):
        sample_schema_url = "https://schema.humancellatlas.org/bundle/5.0.0/biomaterial"
        sample_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse"
        }

        with self.assertRaisesRegex(Exception,
                                    "Only one of function arguments metadata_schema_urls or json_schema_docs [^/]* "
                                    "may be populated"):
            SchemaTemplate(metadata_schema_urls=[sample_schema_url], json_schema_docs=[sample_schema_json])

    @patch("requests.get")
    def test__creation_of_template_with_json__success(self, property_migrations_request_mock):
        property_migrations_request_mock.return_value = Mock(ok=True)
        property_migrations_request_mock.return_value.json.return_value = {'migrations': []}

        sample_metadata_schema_json = {
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

        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        expected_schema_version = "1.0.0"
        expected_schema_metadata_properties = {
            "timecourse": SchemaParser(sample_metadata_schema_json).schema_dictionary}
        expected_schema_labels = {"timecourse.value": ["timecourse.value"], "timecourse.unit": ["timecourse.unit"],
                                  "timecourse value": ["timecourse.value"], "timecourse unit": ["timecourse.unit"]}
        expected_schema_tabs = [{"timecourse": {"display_name": "Timecourse",
                                                "columns": ["timecourse.value", "timecourse.unit"]}}]

        self.assertEqual(schema_template.template_version, expected_schema_version)
        self.assertEqual(schema_template.meta_data_properties, expected_schema_metadata_properties)
        self.assertEqual(schema_template.labels, expected_schema_labels)
        self.assertEqual(schema_template.tabs, expected_schema_tabs)

    @patch("requests.get")
    def test__get_list_of_schema_spreadsheet_representations__success(self, property_migrations_request_mock):
        property_migrations_request_mock.return_value = Mock(ok=True)
        property_migrations_request_mock.return_value.json.return_value = {'migrations': []}

        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
            "description": "Information relating to a timecourse.",
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

        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json], property_migrations=[])

        expected_schema_spreadsheet_tab_representations = [{"timecourse": {"display_name": "Timecourse",
                                                                           "columns": ["timecourse.value",
                                                                                       "timecourse.unit"]}}]
        self.assertEqual(schema_template.get_list_of_schema_spreadsheet_representations(),
                         expected_schema_spreadsheet_tab_representations)

    @patch("requests.get")
    def test__lookup_property_in_schema__success(self, property_migrations_request_mock):
        property_migrations_request_mock.return_value = Mock(ok=True)
        property_migrations_request_mock.return_value.json.return_value = {'migrations': []}

        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
            "description": "Information relating to a timecourse.",
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

        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        expected_property_value = SimplePropertyDescriptor({
            "description": "The unit in which the Timecourse value is expressed.",
            "type": "object",
            "user_friendly": "Timecourse unit"
        })
        self.assertEqual(schema_template.lookup_property_attributes_in_metadata("timecourse.unit"),
                         expected_property_value.get_dictionary_representation_of_descriptor())

    @patch("requests.get")
    def test__lookup_unknown_property_in_schema__throws_exception(self, property_migrations_request_mock):
        property_migrations_request_mock.return_value = Mock(ok=True)
        property_migrations_request_mock.return_value.json.return_value = {'migrations': []}

        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
            "description": "Information relating to a timecourse.",
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
            }
        }

        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        with self.assertRaisesRegex(UnknownKeySchemaException, "Cannot find key"):
            schema_template.lookup_property_attributes_in_metadata("timecourse.unit")

    def test__lookup_next_latest_key_migration_simple_migration__success(self):
        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
            "description": "Information relating to a timecourse.",
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
            }
        }
        sample_property_migration = {
            "source_schema": "timecourse",
            "property": "value",
            "target_schema": "fancy_new_timecourse",
            "replaced_by": "fancy_new_value",
            "effective_from": "3.0.0",
            "reason": "For fun",
            "type": "renamed property"
        }

        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json],
                                         property_migrations=[sample_property_migration])

        expected_replacement_key = "fancy_new_timecourse.fancy_new_value"
        self.assertEqual(schema_template.lookup_next_latest_key_migration("timecourse.value"), expected_replacement_key)

    def test__lookup_absolute_latest_key_migration__success(self):
        sample_metadata_schema_json = [
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
                "description": "Information relating to a timecourse.",
                "type": "object",
                "properties": {
                    "value": {
                        "description": "The numerical value in Timecourse unit",
                        "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                        "type": "string",
                        "example": "2; 5.5-10.5",
                        "user_friendly": "Timecourse value",
                        "guidelines": "Enter either a single value or a range of values. Indicate a range using a "
                                      "hyphen."
                    },
                }
            },
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/fancy_new_timecourse",
                "description": "Information relating to a timecourse.",
                "type": "object",
                "properties": {
                    "fancy_new_value": {
                        "description": "The numerical value in Timecourse unit",
                        "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                        "type": "string",
                        "example": "2; 5.5-10.5",
                        "user_friendly": "Timecourse value",
                        "guidelines": "Enter either a single value or a range of values. Indicate a range using a "
                                      "hyphen."
                    },
                }
            },
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/extra_fancy_new_timecourse",
                "description": "Information relating to a timecourse.",
                "type": "object",
                "properties": {
                    "extra_fancy_new_value": {
                        "description": "The numerical value in Timecourse unit",
                        "pattern": "^[0-9]+\\.?[0-9]*-?[0-9]*\\.?[0-9]*$",
                        "type": "string",
                        "example": "2; 5.5-10.5",
                        "user_friendly": "Timecourse value",
                        "guidelines": "Enter either a single value or a range of values. Indicate a range using a "
                                      "hyphen."
                    },
                }
            }
        ]
        sample_property_migration = [{
            "source_schema": "timecourse",
            "property": "value",
            "target_schema": "fancy_new_timecourse",
            "replaced_by": "fancy_new_value",
            "effective_from": "3.0.0",
            "reason": "For fun",
            "type": "renamed property"
        },
            {"source_schema": "fancy_new_timecourse",
             "property": "fancy_new_value",
             "target_schema": "extra_fancy_new_timecourse",
             "replaced_by": "extra_fancy_new_value",
             "effective_from": "3.0.0",
             "reason": "For fun",
             "type": "renamed property"}]

        schema_template = SchemaTemplate(json_schema_docs=sample_metadata_schema_json,
                                         property_migrations=sample_property_migration)

        expected_replacement_key = "extra_fancy_new_timecourse.extra_fancy_new_value"
        self.assertEqual(schema_template.lookup_absolute_latest_key_migration("timecourse.value"),
                         expected_replacement_key)

    def test__generate_yaml_tabs_only__success(self):
        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
            "description": "Information relating to a timecourse.",
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
            }
        }
        sample_property_migration = {
            "source_schema": "timecourse",
            "property": "value",
            "target_schema": "fancy_new_timecourse",
            "replaced_by": "fancy_new_value",
            "effective_from": "3.0.0",
            "reason": "For fun",
            "type": "renamed property"
        }
        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json],
                                         property_migrations=[sample_property_migration])

        yaml_representation = schema_template.generate_yaml_representation_of_spreadsheets(tabs_only=True)

        self.assertEqual(SortedDict(yaml.load(yaml_representation, Loader=yaml.FullLoader)),
                         SortedDict({"tabs": schema_template.tabs}))

    def test__generate_yaml_full_schema_template__success(self):
        sample_metadata_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse",
            "description": "Information relating to a timecourse.",
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
            }
        }
        sample_property_migration = {
            "source_schema": "timecourse",
            "property": "value",
            "target_schema": "fancy_new_timecourse",
            "replaced_by": "fancy_new_value",
            "effective_from": "3.0.0",
            "reason": "For fun",
            "type": "renamed property"
        }
        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json],
                                         property_migrations=[sample_property_migration])

        yaml_representation = schema_template.generate_yaml_representation_of_spreadsheets(tabs_only=False)

        self.assertEqual(SortedDict(yaml.load(yaml_representation, Loader=yaml.FullLoader)),
                         SortedDict(schema_template.get_dictionary_representation()))
