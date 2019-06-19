#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "19/06/2019"

import os
import unittest
from unittest import TestCase

import tests.template.schema_mock_utils as schema_mock
from ingest.template.schema_template import RootSchemaException, SchemaParser, UnknownKeyException


class TestSchemaTemplate(TestCase):

    def setUp(self):
        self.longMessage = True
        self.dummyCellSuspension = "https://schema.humancellatlas.org/type/biomaterial/13.1.1/cell_suspension"
        pass

    def test_migration_lookup(self):
        data = '{"id" : "' + self.dummyCellSuspension + '", "properties": {"selected_cell_types" : {"user_friendly": "Selected cell type(s)"}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("Selected cell type(s)", template.lookup('cell_suspension.selected_cell_types.user_friendly'))


        with self.assertRaises(UnknownKeyException):
            self.assertTrue(template.lookup("cell_suspension.selected_cell_type"))

        self.assertEqual("cell_suspension.selected_cell_types", template.replaced_by('cell_suspension.selected_cell_type'))


        # print(template.lookup('cell_suspension.selected_cell_types'))

        # self.assertEqual("project", template.lookup('cell_suspension.selected_cell_type'))
        # self.assertEqual("project", template.lookup('project.schema.module'))
        # self.assertEqual("type", template.lookup('project.schema.high_level_entity'))

    def test_backtrack_lookup(self):

        data = '{"id" : "' + self.dummyCellSuspension + '", "properties": {"selected_cell_types" : {"user_friendly": "Selected cell type(s)"}} }'
        template = schema_mock.get_template_for_json(data=data)

        self.assertEqual("cell_suspension.selected_cell_types.ontology", template.replaced_by('cell_suspension.selected_cell_type.ontology'))


if __name__ == '__main__':
    unittest.main()
