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
from unittest.mock import patch, MagicMock

from ingest.template.schema_template import SchemaParser
from ingest.template.schema_template import SchemaTemplate
from ingest.template.schema_template import UnknownKeyException
from ingest.template.schema_template import RootSchemaException

class TestSchemaTemplate(TestCase):
    def setUp(self):
        self.longMessage = True
        self.dummyProjectUri = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.dummyProjectUri = "https://schema.humancellatlas.org/type/biomaterial/5.1.0/donor_organism"
        pass

    # def test_load_schema(self):
    #     schema_parser = SchemaParser()
    #     dirname = os.path.dirname(os.path.realpath(__file__))
    #
    #     with open(os.path.join(dirname,"data","project_type.json")) as json_file:
    #         json_data = json.load(json_file)
    #         schema_template = schema_parser._load_schema(json_data)
    #         schema_template.json_dump()
    #         print (schema_template.yaml_dump())
    #     pass

    # def test_get_schema_properties_from_object(self):
    #     schema_parser = SchemaParser()
    #     schema_parser.
    #     self.assertIn("foo", schema_parser._get_schema_properties_from_object({'properties': {'foo': 'bar'} }).keys())
    #     self.assertIn("bar", schema_parser._get_schema_properties_from_object({'properties': { 'foo': 'bar', 'bar' : 'foo' } }).keys())
    #     self.assertFalse(schema_parser._get_schema_properties_from_object({'foobar': {'foo': 'bar'} }).keys())
    #     self.assertFalse(schema_parser._get_schema_properties_from_object({'properties': {} }).keys())
    #     self.assertFalse(schema_parser._get_schema_properties_from_object({'properties': [] }).keys())
    #     self.assertFalse(schema_parser._get_schema_properties_from_object({'properties': '' }).keys())
    #     self.assertIn("foo", schema_parser._get_schema_properties_from_object({'items': {'properties': {'foo': 'bar'}}}))

    @patch('urllib.request.urlopen')
    def get_template_for_json(self, mock_urlopen, data="{}"):
        cm = MagicMock()
        cm.getcode.return_value = 200
        cm.read.return_value = data.encode()
        cm.__enter__.return_value = cm
        mock_urlopen.return_value = cm

        return SchemaTemplate(['test_url'])

    def test_schema_lookup(self):

        data='{"id" : "'+self.dummyProjectUri+'", "properties": {"foo": "bar"} }'
        template = self.get_template_for_json(data=data)
        self.assertEqual("project", template.lookup('project.schema.domain_entity'))
        self.assertEqual("project", template.lookup('project.schema.module'))
        self.assertEqual("type", template.lookup('project.schema.high_level_entity'))

    def test_no_root_schema(self):
        data = '{"properties": {"foo": "bar"} }'
        with self.assertRaises(RootSchemaException):
            self.get_template_for_json(data=data)


    def test_unknown_key_exception(self):
        data = '{"id" : "'+self.dummyProjectUri+'", "properties": {"foo": "bar"} }'
        template = self.get_template_for_json(data=data)
        with self.assertRaises(UnknownKeyException):
                template.lookup('foo')

    def test_get_tab_name(self):
        data = '{"id" : "' + self.dummyProjectUri + '", "properties": {"foo": "bar"} }'
        template = self.get_template_for_json(data=data)

        tabs = template.get_tabs_config()
        self.assertEqual("donor_organism", tabs.get_key_for_label("Donor organism"))


    def test_get_domain_entity_from_url(self):
        template = SchemaTemplate([])
        schema_parser = SchemaParser(template)
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("project", schema_parser.get_domain_entity_from_url(url))
        url = "https://schema.humancellatlas.org/type/foo/bar/5.1.0/project"
        self.assertEqual("foo/bar", schema_parser.get_domain_entity_from_url(url))

    def test_get_high_level_entity_from_url(self):
        template = SchemaTemplate([])
        schema_parser = SchemaParser(template)
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("type", schema_parser.get_high_level_entity_from_url(url))

    def test_get_module_from_url(self):
        template = SchemaTemplate([])
        schema_parser = SchemaParser(template)
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("project", schema_parser.get_module_from_url(url))

if __name__ == '__main__':
    unittest.main()