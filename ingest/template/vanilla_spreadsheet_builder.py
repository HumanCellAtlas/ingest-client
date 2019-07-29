#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Excel format.
"""

from .spreadsheet_builder import SpreadsheetBuilder

# TODO(maniarathi): Consolidate default values into a shared configuration file.
DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"


class VanillaSpreadsheetBuilder(SpreadsheetBuilder):
    def __init__(self, output_file, hide_row=False):
        super(VanillaSpreadsheetBuilder, self).create_initial_spreadsheet(output_file, hide_row)

    def build(self, spreadsheet_tabs_template):
        tabs = spreadsheet_tabs_template.tab_config

        for tab in tabs.lookup("tabs"):
            for tab_name, detail in tab.items():

                worksheet = self.spreadsheet.add_worksheet(detail["display_name"])

                for column_index, column_name in enumerate(detail["columns"]):

                    formatted_column_name = self.get_user_friendly_column_name(spreadsheet_tabs_template,
                                                                               column_name, tab_name).upper()

                    if column_name.split(".")[-1] == "text":
                        desc = self.get_value_for_column(spreadsheet_tabs_template, column_name.replace('.text', ''),
                                                         "description")
                        if desc == "":
                            desc = self.get_value_for_column(spreadsheet_tabs_template, column_name, "description")
                    else:
                        if column_name + ".text" not in detail["columns"]:
                            desc = self.get_value_for_column(spreadsheet_tabs_template, column_name, "description")
                    if column_name.split(".")[-1] == "text":
                        required = bool(
                            self.get_value_for_column(spreadsheet_tabs_template, column_name.replace('.text', ''),
                                                      "required"))
                    else:
                        if column_name + ".text" not in detail["columns"]:
                            required = bool(
                                self.get_value_for_column(spreadsheet_tabs_template, column_name, "required"))
                    if column_name.split(".")[-1] == "text":
                        example_text = self.get_value_for_column(spreadsheet_tabs_template,
                                                                 column_name.replace('.text', ''), "example")
                        if example_text == "":
                            example_text = self.get_value_for_column(spreadsheet_tabs_template, column_name, "example")
                    else:
                        if column_name + ".text" not in detail["columns"]:
                            example_text = self.get_value_for_column(spreadsheet_tabs_template, column_name, "example")
                    if column_name.split(".")[-1] == "text":
                        guidelines = self.get_value_for_column(spreadsheet_tabs_template,
                                                               column_name.replace('.text', ''), "guidelines")
                        if guidelines == "":
                            guidelines = self.get_value_for_column(spreadsheet_tabs_template, column_name, "guidelines")
                    else:
                        if column_name + ".text" not in detail["columns"]:
                            guidelines = self.get_value_for_column(spreadsheet_tabs_template, column_name, "guidelines")

                    if required:
                        formatted_column_name += " (Required)"

                    # set the user friendly name
                    worksheet.write(0, column_index, formatted_column_name, self.header_format)

                    if len(formatted_column_name) < 25:
                        col_w = 25
                    else:
                        col_w = len(formatted_column_name)

                    worksheet.set_column(column_index, column_index, col_w)

                    # set the description
                    worksheet.write(1, column_index, desc, self.desc_format)

                    # write example
                    if example_text:
                        worksheet.write(2, column_index, guidelines + ' For example: ' + example_text, self.desc_format)
                    else:
                        worksheet.write(2, column_index, guidelines, self.desc_format)

                    # set the key
                    worksheet.write(3, column_index, column_name, self.locked_format)

                    if column_name.split(".")[-1] == "ontology" or column_name.split(".")[-1] == "ontology_label":
                        worksheet.set_column(column_index, column_index, None, None, {'hidden': True})

                    if self.hidden_row:
                        worksheet.set_row(3, None, None, {'hidden': True})

                    if column_index == 0:
                        worksheet.set_row(0, 30)
                        worksheet.set_row(4, 30)

                        worksheet.write(4, column_index, "FILL OUT INFORMATION BELOW THIS ROW", self.header_format)

                    else:
                        worksheet.write(4, column_index, '', self.header_format)

        if self.include_schemas_tab:
            self.generate_and_add_schema_worksheet_to_spreadsheet(spreadsheet_tabs_template.get_schema_urls())
