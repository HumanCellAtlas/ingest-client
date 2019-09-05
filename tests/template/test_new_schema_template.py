import unittest

from ingest.template.descriptor import SimplePropertyDescriptor
from ingest.template.exceptions import UnknownKeySchemaException
from ingest.template.new_schema_parser import NewSchemaParser
from ingest.template.new_schema_template import NewSchemaTemplate


class TestNewSchemaTemplate(unittest.TestCase):
    """ Testing class for the NewSchemaTemplateclass. """

    def test__creation_of_template_with_urls_and_jsons__throws_exception(self):
        sample_schema_url = "https://schema.humancellatlas.org/bundle/5.0.0/biomaterial"
        sample_schema_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://schema.humancellatlas.org/module/biomaterial/2.0.2/timecourse"
        }

        with self.assertRaisesRegex(Exception,
                                    "Only one of function arguments metadata_schema_urls or json_schema_docs [^/]* "
                                    "may be populated"):
            NewSchemaTemplate(metadata_schema_urls=[sample_schema_url], json_schema_docs=[sample_schema_json])

    def test__creation_of_template_with_json__success(self):
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

        schema_template = NewSchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        expected_schema_version = "1.0.0"
        expected_schema_metadata_properties = {
            "timecourse": NewSchemaParser(sample_metadata_schema_json).schema_dictionary}
        expected_schema_labels = {"timecourse.value": ["timecourse.value"], "timecourse.unit": ["timecourse.unit"],
                                  "timecourse value": ["timecourse.value"], "timecourse unit": ["timecourse.unit"]}
        expected_schema_tabs = [{"timecourse": {"display_name": "Timecourse"},
                                 "columns": ["timecourse.value", "timecourse.unit"]}]

        self.assertEqual(schema_template.template_version, expected_schema_version)
        self.assertEqual(schema_template.meta_data_properties, expected_schema_metadata_properties)
        self.assertEqual(schema_template.labels, expected_schema_labels)
        self.assertEqual(schema_template.tabs, expected_schema_tabs)

    def test__lookup_property_in_schema__success(self):
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

        schema_template = NewSchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        expected_property_value = SimplePropertyDescriptor({
            "description": "The unit in which the Timecourse value is expressed.",
            "type": "object",
            "user_friendly": "Timecourse unit"
        })
        self.assertEqual(schema_template.lookup_property_attributes_in_metadata("timecourse.unit"),
                         expected_property_value.get_dictionary_representation_of_descriptor())

    def test__lookup_unknown_property_in_schema__throws_exception(self):
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

        schema_template = NewSchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        with self.assertRaisesRegex(UnknownKeySchemaException, "Cannot find key"):
            schema_template.lookup_property_attributes_in_metadata("timecourse.unit")
