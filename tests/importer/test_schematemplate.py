#!/usr/bin/env python
"""
Description goes here
"""

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "01/05/2018"

import os
import json
import unittest
from unittest import TestCase

from ingest.template.schematemplate import SchemaParser

class TestSchemaTemplate(TestCase):
    def setUp(self):
        self.longMessage = True
        pass

    def test_load_schema(self):
        schema_parser = SchemaParser()
        dirname = os.path.dirname(os.path.realpath(__file__))

        with open(os.path.join(dirname,"data","project_type.json")) as json_file:
            json_data = json.load(json_file)
            schema_template = schema_parser.load_schema(json_data)
            schema_template.json_dump()
            print (schema_template.yaml_dump())
        pass

    def test_get_schema_properties_from_object(self):
        schema_parser = SchemaParser()
        self.assertIn("foo", schema_parser._get_schema_properties_from_object({'properties': {'foo': 'bar'} }).keys())
        self.assertIn("bar", schema_parser._get_schema_properties_from_object({'properties': { 'foo': 'bar', 'bar' : 'foo' } }).keys())
        self.assertFalse(schema_parser._get_schema_properties_from_object({'foobar': {'foo': 'bar'} }).keys())
        self.assertFalse(schema_parser._get_schema_properties_from_object({'properties': {} }).keys())
        self.assertFalse(schema_parser._get_schema_properties_from_object({'properties': [] }).keys())
        self.assertFalse(schema_parser._get_schema_properties_from_object({'properties': '' }).keys())
        self.assertIn("foo", schema_parser._get_schema_properties_from_object({'items': {'properties': {'foo': 'bar'}}}))

    def test_get_domain_entity_from_url(self):
        schema_parser = SchemaParser()
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("project", schema_parser.get_domain_entity_from_url(url))
        url = "https://schema.humancellatlas.org/type/foo/bar/5.1.0/project"
        self.assertEqual("foo/bar", schema_parser.get_domain_entity_from_url(url))

    def test_get_high_level_entity_from_url(self):
        schema_parser = SchemaParser()
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("type", schema_parser.get_high_level_entity_from_url(url))

    def test_get_module_from_url(self):
        schema_parser = SchemaParser()
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("project", schema_parser.get_module_from_url(url))

if __name__ == '__main__':
    unittest.main()