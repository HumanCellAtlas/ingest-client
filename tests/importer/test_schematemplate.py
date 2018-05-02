#!/usr/bin/env python
"""
Description goes here
"""
from unittest.case import _SubTest

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "01/05/2018"

import os
import json

from unittest import TestCase

from ingest.importer.schematemplate import SchemaTemplate


class TestSchemaTemplate(TestCase):
    def setUp(self):
        pass


    def test_load_schema(self):
        schemaTemplate = SchemaTemplate()

        dirname = os.path.dirname(os.path.realpath(__file__))

        with open(os.path.join(dirname,"data","json_schema_1.json")) as json_file:
            json_data = json.load(json_file)

            schemaTemplate.load_schema(json_data)

        pass


    def test___get_schema_properties_from_object(self):
        schemaTemplate = SchemaTemplate()
        self.assertIn("foo", schemaTemplate._get_schema_properties_from_object({'properties': {'foo': 'bar'} }))
        self.assertIn("bar", schemaTemplate._get_schema_properties_from_object({'properties': { 'foo': 'bar', 'bar' : 'foo' } }))
        self.assertFalse(schemaTemplate._get_schema_properties_from_object({'foobar': {'foo': 'bar'} }))
        self.assertFalse(schemaTemplate._get_schema_properties_from_object({'properties': {} }))
        self.assertFalse(schemaTemplate._get_schema_properties_from_object({'properties': [] }))
        self.assertFalse(schemaTemplate._get_schema_properties_from_object({'properties': '' }))

