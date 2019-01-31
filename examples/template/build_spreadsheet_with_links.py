#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "31/01/2019"

from ingest.template.linked_sheet_builder import LinkedSheetBuilder












# build a generic spreadsheet from the latest schemas
spreadsheet_builder = LinkedSheetBuilder("generic_with_links.xlsx")
spreadsheet_builder.generate_workbook()
spreadsheet_builder.save_workbook()