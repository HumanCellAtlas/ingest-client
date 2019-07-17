#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "19/06/2019"

import unittest
from unittest import TestCase

import tests.template.schema_mock_utils as schema_mock
from ingest.template.exceptions import UnknownKeySchemaException


class TestSchemaTemplate(TestCase):

    def setUp(self):
        self.longMessage = True
        self.dummyCellSuspension = "https://schema.humancellatlas.org/type/biomaterial/13.1.1/cell_suspension"
        pass

    def test_migration_lookup(self):
        data = '{"id" : "' + self.dummyCellSuspension + '", "properties": {"selected_cell_types" : {"user_friendly": ' \
                                                        '"Selected cell type(s)"}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("Selected cell type(s)", template.lookup('cell_suspension.selected_cell_types.user_friendly'))

        with self.assertRaises(UnknownKeySchemaException):
            self.assertTrue(template.lookup("cell_suspension.selected_cell_type"))

        self.assertEqual("cell_suspension.selected_cell_types",
                         template.replaced_by('cell_suspension.selected_cell_type'))

    def test_backtrack_lookup(self):
        data = '{"id" : "' + self.dummyCellSuspension + '", "properties": {"selected_cell_types" : {"user_friendly": ' \
                                                        '"Selected cell type(s)"}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("cell_suspension.selected_cell_types.ontology",
                         template.replaced_by('cell_suspension.selected_cell_type.ontology'))
        self.assertEqual("cell_suspension.selected_cell_types.text.user_friendly",
                         template.replaced_by('cell_suspension.selected_cell_type.text.user_friendly'))

    def test_version_lookups(self):
        data = '{"id" : "' + self.dummyCellSuspension + '", "properties": {"selected_cell_types_foo" : {' \
                                                        '"user_friendly": "Selected cell type(s)"}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("cell_suspension.selected_cell_types_foo",
                         template.replaced_by_latest('cell_suspension.selected_cell_type'))
        self.assertEqual("cell_suspension.selected_cell_types",
                         template.replaced_by_at('cell_suspension.selected_cell_type', "13.5.2"))
        self.assertEqual("cell_suspension.selected_cell_types",
                         template.replaced_by_at('cell_suspension.selected_cell_type', "14.5.2"))
        self.assertEqual("cell_suspension.selected_cell_types_foo",
                         template.replaced_by_at('cell_suspension.selected_cell_types_foo', "16.5.2"))
        self.assertEqual("project.contributors.project_role.text",
                         template.replaced_by_at('project.contributors.project_role', "14.0.0"))
        self.assertEqual("project.contributors.project_role",
                         template.replaced_by_at('project.contributors.project_role', "9.0.0"))
        self.assertEqual("analysis_protocol.protocol_core.protocol_id",
                         template.replaced_by_at('analysis_protocol.protocol_core.protocol_id', "9.0.0"))

    def test_migration_failures(self):
        data = '{"id" : "' + self.dummyCellSuspension + '", "properties": {"selected_cell_types_foo" : {' \
                                                        '"user_friendly": "Selected cell type(s)"}} }'
        template = schema_mock.get_template_for_json(data=data)

        with self.assertRaises(UnknownKeySchemaException):
            self.assertTrue(template.lookup("foo.bar"))

        with self.assertRaises(UnknownKeySchemaException):
            self.assertTrue(template.replaced_by_latest('foo.bar'))

        self.assertEqual("foo.bar", template.replaced_by_at('foo.bar', "12.0.2"))
        self.assertEqual("foo.bar", template.replaced_by('foo.bar'))


if __name__ == '__main__':
    unittest.main()
