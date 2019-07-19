#!/usr/bin/env python
"""
A generic executor class to generate empty spreadsheets based on a set of metadata schemas.
"""

from argparse import ArgumentParser

from spreadsheet_builder_constants import DEFAULT_AUTOFILL_SCALE, DEFAULT_LINK_CONFIG, DEFAULT_SCHEMA_LIST

from ingest.template.linked_spreadsheet_builder import LinkedSpreadsheetBuilder
from ingest.template.vanilla_spreadsheet_builder import VanillaSpreadsheetBuilder


def execute_spreadsheet_building(output_file_name, hide_row=False, linked_spreadsheet=False, schema_urls=None,
                                 tabs_template=None):
    """
    Creates and saves a spreadsheet.

    :param output_file_name: A string representing the name of spreadsheet that will be saved to disk.
    :param hide_row: A boolean where if True, will hide the third row of the generated spreadsheet which contains the
                     fully qualified names of each of the fields in the metadata schema.
    :param linked_spreadsheet: A boolean where if true, generates a spreadsheet where columns (a.k.a. metadata
                               fields) from one spreadsheet is copied over to another spreadsheet due to relationships
                               between the schemas (i.e. a donor_id is copied over to a specimen_from_organism tab from
                               the donor_organism tab).
    :param schema_urls: A list of strings where each string represents a URL containing a JSON-formatted metadata
                        schema.
    :param tabs_template: A string representing a YAML file that contains a configuration specifying how the tabs in
                          the generated spreadsheet should look and what information/columns it should contain.
    """

    if linked_spreadsheet:
        spreadsheet_builder = LinkedSpreadsheetBuilder(output_file_name, hide_row, link_config=DEFAULT_LINK_CONFIG,
                                                       autofill_scale=DEFAULT_AUTOFILL_SCALE)
    else:
        spreadsheet_builder = VanillaSpreadsheetBuilder(output_file_name, hide_row)

    spreadsheet_builder.generate_spreadsheet(schema_urls=schema_urls if schema_urls else DEFAULT_SCHEMA_LIST,
                                             tabs_template=tabs_template)
    spreadsheet_builder.save_spreadsheet()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-o", "--output", dest="output", default="default_spreadsheet.xlsx",
                        help="Name of the output spreadsheet")
    parser.add_argument("-r", "--hidden_row", action="store_true",
                        help="Binary flag - if set, the 4th row will be hidden")
    parser.add_argument("-l", "--linked", dest="linked_spreadsheet", action="store_true",
                        help="Boolean flag - if set to true, will generate a linked spreadsheet")
    parser.add_argument("-y", "--yaml", dest="yaml", default=None,
                        help="The YAML file from which to generate the spreadsheet")
    parser.add_argument("-u", "--url", dest="url",
                        help="Optional ingest API URL - if not default (prod)")

    args = parser.parse_args()

    execute_spreadsheet_building(output_file_name=args.output, hide_row=args.hidden_row,
                                 linked_spreadsheet=args.linked_spreadsheet, tabs_template=args.yaml)
