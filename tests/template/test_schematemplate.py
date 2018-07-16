#!/usr/bin/env python
"""
Description goes here
"""

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "01/05/2018"

import unittest
from unittest import TestCase
from unittest.mock import patch, MagicMock

import tests.template.schema_mock_utils as schema_mock

from ingest.template.schema_template import SchemaParser
from ingest.template.schema_template import SchemaTemplate
from ingest.template.schema_template import UnknownKeyException
from ingest.template.schema_template import RootSchemaException

class TestSchemaTemplate(TestCase):
    def setUp(self):
        self.longMessage = True
        self.dummyProjectUri = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.dummyDonorUri = "https://schema.humancellatlas.org/type/biomaterial/5.1.0/donor_organism"
        pass

    def test_schema_lookup(self):

        data='{"id" : "'+self.dummyProjectUri+'", "properties": {"foo": "bar"} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertEqual("project", template.lookup('project.schema.domain_entity'))
        self.assertEqual("project", template.lookup('project.schema.module'))
        self.assertEqual("type", template.lookup('project.schema.high_level_entity'))

    def test_no_root_schema(self):
        data = '{"properties": {"foo": "bar"} }'
        with self.assertRaises(RootSchemaException):
            schema_mock.get_template_for_json(data=data)


    def test_unknown_key_exception(self):
        data = '{"id" : "'+self.dummyProjectUri+'", "properties": {"foo": "bar"} }'
        template = schema_mock.get_template_for_json(data=data)
        with self.assertRaises(UnknownKeyException):
                template.lookup('foo')

    def test_get_tab_name(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)

        tabs = template.get_tabs_config()
        self.assertEqual("donor_organism", tabs.get_key_for_label("donor_organism"))
        self.assertEqual("donor_organism", tabs.get_key_for_label("Donor organism"))

    def test_lookup_key_in_tab(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("donor_organism.foo_bar", template.get_key_for_label("Foo bar", "Donor organism"))

        with self.assertRaises(UnknownKeyException):
            template.get_key_for_label("Bar foo", "Donor organism")

    def test_required_fields(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "required": ["foo_bar"], "properties": { "foo_bar": {"user_friendly" : "Foo bar"}, "bar_foo" : {}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertTrue(template.lookup("donor_organism.foo_bar.required"))
        self.assertFalse(template.lookup("donor_organism.bar_foo.required"))

    def test_has_type(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": { "foo_bar": {"type" : "string"} } }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertEqual("string", template.lookup("donor_organism.foo_bar.value_type"))
        self.assertFalse(template.lookup("donor_organism.foo_bar.multivalue"))


    def test_has_type_list(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": { "foo_bar": {"type" : "array" , "items" : {"type": "string"}} } }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertEqual("string", template.lookup("donor_organism.foo_bar.value_type"))
        self.assertTrue(template.lookup("donor_organism.foo_bar.multivalue"))

    def test_identifiable(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"biomaterial_id": {"identifiable" : "true"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertTrue(template.lookup("donor_organism.biomaterial_id.identifiable"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertFalse(template.lookup("donor_organism.foo_bar.identifiable"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"process_id": {"identifiable" : "true"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertTrue(template.lookup("donor_organism.process_id.identifiable"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"protocol_id": {"identifiable" : "true"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertTrue(template.lookup("donor_organism.protocol_id.identifiable"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"file_name": {"identifiable" : "true"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertTrue(template.lookup("donor_organism.file_name.identifiable"))

    def test_get_key_for_label(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)

        tabs = template.get_tabs_config()
        self.assertEqual("donor_organism", tabs.get_key_for_label("donor_organism"))

    def test_description(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"description" : "Foo is a bar"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertEqual("Foo is a bar", template.lookup("donor_organism.foo_bar.description"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertIsNone(template.lookup("donor_organism.foo_bar.description"))

    def test_format(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"format" : "date-time"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertEqual("date-time", template.lookup("donor_organism.foo_bar.format"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertIsNone(template.lookup("donor_organism.foo_bar.format"))

    def test_retrieveable(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"format" : "date-time"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertTrue(template.lookup("donor_organism.uuid.external_reference"))
        self.assertTrue(template.lookup("donor_organism.uuid.identifiable"))
        self.assertFalse(template.lookup("donor_organism.foo_bar.external_reference"))
        self.assertFalse(template.lookup("donor_organism.foo_bar.identifiable"))

        with self.assertRaises(UnknownKeyException):
            self.assertTrue(template.lookup("donor_organism.format.uuid.retrievable"))



    def test_example(self):
        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"example" : "Foo is a bar"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertEqual("Foo is a bar", template.lookup("donor_organism.foo_bar.example"))

        data = '{"id" : "' + self.dummyDonorUri + '", "properties": {"foo_bar": {"user_friendly" : "Foo bar"}} }'
        template = schema_mock.get_template_for_json(data=data)
        self.assertIsNone(template.lookup("donor_organism.foo_bar.example"))

    def test_follows_item_refs(self):
        data = '{"id" : "' + self.dummyProjectUri + '", "properties": { "foo_bar": {"type" : "array" , "items" : {"type" : "object", "id": "'+self.dummyDonorUri+'"}} } }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("biomaterial", template.lookup("project.foo_bar.schema.domain_entity"))

    def test_get_domain_entity_from_url(self):
        schema_parser = SchemaParser(None)
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("project", schema_parser.get_domain_entity_from_url(url))
        url = "https://schema.humancellatlas.org/type/foo/bar/5.1.0/project"
        self.assertEqual("foo/bar", schema_parser.get_domain_entity_from_url(url))
        url = "https://schema.humancellatlas.org/type/foo/bar/latest/project"
        self.assertEqual("foo/bar", schema_parser.get_domain_entity_from_url(url))
        url = "https://schema.humancellatlas.org/type/foo/latest/project"
        self.assertEqual("foo", schema_parser.get_domain_entity_from_url(url))
        url = 'https://schema.humancellatlas.org/bundle/1.0.0/links'
        self.assertEqual(None, schema_parser.get_domain_entity_from_url(url))
        url= 'http://schema.dev.data.humancellatlas.org/system/1.0.0/provenance'
        self.assertEqual(None, schema_parser.get_domain_entity_from_url(url))

    def test_get_high_level_entity_from_url(self):
        schema_parser = SchemaParser(None)
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("type", schema_parser.get_high_level_entity_from_url(url))
        url = 'http://schema.dev.data.humancellatlas.org/system/1.0.0/provenance'
        self.assertEqual('system', schema_parser.get_high_level_entity_from_url(url))

    def test_get_module_from_url(self):
        schema_parser = SchemaParser(None)
        url = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.assertEqual("project", schema_parser.get_module_from_url(url))

if __name__ == '__main__':
    unittest.main()