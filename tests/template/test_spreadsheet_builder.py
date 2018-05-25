#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "25/05/2018"

from unittest import TestCase
import ingest.template.spreadsheet_builder as spreadsheet_builder

class TestSchemaTemplate(TestCase):
    def setUp(self):
        self.longMessage = True
        self.dummyProjectUri = "https://schema.humancellatlas.org/type/project/5.1.0/project"
        self.dummyDonorUri = "https://schema.humancellatlas.org/type/biomaterial/5.1.0/donor_organism"
        pass


    def test_no_schemas(self):
        pass

    def test_with_tabs_template(self):
        pass
        # spreadsheet_builder.generate_spreadsheet("human_10x.xlsx", tabs_template="tabs_human_10x.yaml",
        #                                          schema_urls=schemas)

    def test_add_columns(self):
        # spreadsheet_builder.generate_spreadsheet("generic.xlsx", schema_urls=schemas)
        pass

    def test_add_sheets(self):
        # spreadsheet_builder.generate_spreadsheet("generic.xlsx", schema_urls=schemas)
        pass
