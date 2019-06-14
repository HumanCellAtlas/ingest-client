#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
from argparse import ArgumentParser

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

from ingest.template import schema_template
from ingest.template.tabs import TabConfig
import xlsxwriter

DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"
DEFAULT_MIGRATIONS_URL = "https://schema.humancellatlas.org/property_migrations"

class SpreadsheetBuilder:
    def __init__(self, output_file, hide_row=False):

        self.workbook = xlsxwriter.Workbook(output_file)

        self.header_format = self.workbook.add_format(
            {'bold': True, 'bg_color': '#D0D0D0', 'font_size': 12, 'valign': 'vcenter'})
        self.locked_format = self.workbook.add_format({'locked': True})
        # self.required_header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.desc_format = self.workbook.add_format(
            {'font_color': '#808080', 'italic': True, 'text_wrap': True, 'font_size': 12, 'valign': 'top'})
        self.include_schemas_tab = False
        self.hidden_row = hide_row

    def generate_workbook(self, tabs_template=None, schema_urls=list(), include_schemas_tab=False):

        self.include_schemas_tab = include_schemas_tab
        if tabs_template:

            tabs_parser = TabConfig()
            tabs = tabs_parser.load(tabs_template)
            template = schema_template.SchemaTemplate(list_of_schema_urls=schema_urls, tab_config=tabs)
        else:
            template = schema_template.SchemaTemplate(list_of_schema_urls=schema_urls)

        self._build(template)
        return self

    def _get_value_for_column(self, template, col_name, property):
        try:
            uf = str(template.lookup(col_name + "." + property)) if template.lookup(col_name + "." + property) else ""
            return uf
        except Exception:
            print("No property " + property + " for " + col_name)
            return ""

    def get_user_friendly(self, template, col_name):

        if '.text' in col_name:
            parent = col_name.replace('.text', '')
            key = parent + ".user_friendly"
        elif '.ontology_label' in col_name:
            parent = col_name.replace('.ontology_label', '')
            key = parent + ".user_friendly"
        elif '.ontology' in col_name:
            parent = col_name.replace('.ontology', '')
            key = parent + ".user_friendly"

        else:
            key = col_name + ".user_friendly"
        try:
            uf = str(template.lookup(key)) if template.lookup(key) else col_name
            if '.ontology_label' in col_name:
                uf = uf + " ontology label"
            if '.ontology' in col_name:
                uf = uf + " ontology ID"

            return uf
        except Exception:
            return key

    def save_workbook(self):
        self.workbook.close()

    def _write_schemas(self, schema_urls):
        worksheet = self.workbook.add_worksheet("Schemas")
        worksheet.write(0, 0, "Schemas")
        for index, url in enumerate(schema_urls):
            worksheet.write(index + 1, 0, url)

    def _build(self, template):

        tabs = template.get_tabs_config()

        for tab in tabs.lookup("tabs"):

            for tab_name, detail in tab.items():

                worksheet = self.workbook.add_worksheet(detail["display_name"])

                col_number = 0

                for cols in detail["columns"]:

                    if cols.split(".")[-1] == "text":
                        uf = self.get_user_friendly(template, cols.replace('.text', '')).upper()
                    else:
                        if cols + ".text" not in detail["columns"]:
                            uf = self.get_user_friendly(template, cols).upper()
                    if cols.split(".")[-1] == "text":
                        desc = self._get_value_for_column(template, cols.replace('.text', ''), "description")
                        if desc == "":
                            desc = self._get_value_for_column(template, cols, "description")
                    else:
                        if cols + ".text" not in detail["columns"]:
                            desc = self._get_value_for_column(template, cols, "description")
                    if cols.split(".")[-1] == "text":
                        required = bool(self._get_value_for_column(template, cols.replace('.text', ''), "required"))
                    else:
                        if cols + ".text" not in detail["columns"]:
                            required = bool(self._get_value_for_column(template, cols, "required"))
                    if cols.split(".")[-1] == "text":
                        example_text = self._get_value_for_column(template, cols.replace('.text', ''), "example")
                        if example_text == "":
                            example_text = self._get_value_for_column(template, cols, "example")
                    else:
                        if cols + ".text" not in detail["columns"]:
                            example_text = self._get_value_for_column(template, cols, "example")
                    if cols.split(".")[-1] == "text":
                        guidelines = self._get_value_for_column(template, cols.replace('.text', ''), "guidelines")
                        if guidelines == "":
                            guidelines = self._get_value_for_column(template, cols, "guidelines")
                    else:
                        if cols + ".text" not in detail["columns"]:
                            guidelines = self._get_value_for_column(template, cols, "guidelines")

                    hf = self.header_format
                    if required:
                        uf = uf + " (Required)"

                    # set the user friendly name
                    worksheet.write(0, col_number, uf, hf)

                    if len(uf) < 25:
                        col_w = 25
                    else:
                        col_w = len(uf)

                    worksheet.set_column(col_number, col_number, col_w)

                    # set the description
                    worksheet.write(1, col_number, desc, self.desc_format)

                    # write example
                    if example_text:
                        # print("Example " + example_text)
                        worksheet.write(2, col_number, guidelines + ' For example: ' + example_text, self.desc_format)
                    else:
                        # print("Guideline " + guidelines)
                        worksheet.write(2, col_number, guidelines, self.desc_format)

                    # set the key
                    worksheet.write(3, col_number, cols, self.locked_format)

                    if cols.split(".")[-1] == "ontology" or cols.split(".")[-1] == "ontology_label":
                        worksheet.set_column(col_number, col_number, None, None, {'hidden': True})

                    if self.hidden_row:
                        worksheet.set_row(3, None, None, {'hidden': True})

                    if col_number == 0:
                        worksheet.set_row(0, 30)
                        worksheet.set_row(4, 30)

                        worksheet.write(4, col_number, "FILL OUT INFORMATION BELOW THIS ROW", hf)

                    else:
                        worksheet.write(4, col_number, '', hf)

                    col_number += 1

                    # worksheet.merge_range(first_col=0, first_row=4, last_col= len(detail["columns"]), last_row=4,
                    # cell_format= self.header_format, data="FILL OUT INFORMATION BELOW THIS ROW")

        if self.include_schemas_tab:
            self._write_schemas(template.get_schema_urls())

        return self


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-y", "--yaml", dest="yaml",
                        help="The YAML file from which to generate the spreadsheet")
    parser.add_argument("-o", "--output", dest="output",
                        help="Name of the output spreadsheet")
    parser.add_argument("-u", "--url", dest="url",
                        help="Optional ingest API URL - if not default (prod)")
    parser.add_argument("-m", "--migrations", dest="migrations",
                        help="Optional migrations URL - if not default (prod)")
    parser.add_argument("-r", "--hidden_row", action="store_true",
                        help="Binary flag - if set, the 4th row will be hidden")
    args = parser.parse_args()

    if not args.output:
        output_file = "template_spreadsheet.xlsx"
    else:
        output_file = args.output

    if not args.url:
        ingest_url = DEFAULT_INGEST_URL
    else:
        ingest_url = args.url
    schemas_url = ingest_url + DEFAULT_SCHEMAS_ENDPOINT

    if not args.migrations:
        migrations_url = DEFAULT_MIGRATIONS_URL
    else:
        migrations_url = args.migrations

    hide_row = False

    if args.hidden_row:
        hide_row = True

    all_schemas = schema_template.SchemaTemplate(ingest_url, migrations_url).get_schema_urls()

    # all_schemas = [
    #     "http://schema.dev.data.humancellatlas.org/type/project/9.0.5/project",
    #     "http://schema.dev.data.humancellatlas.org/type/biomaterial/8.6.2/cell_suspension",
    #     "http://schema.dev.data.humancellatlas.org/type/biomaterial/6.3.4/specimen_from_organism",
    #     "http://schema.dev.data.humancellatlas.org/type/biomaterial/12.0.0/donor_organism",
    #     "http://schema.dev.data.humancellatlas.org/type/file/1.1.5/supplementary_file",
    #     "http://schema.dev.data.humancellatlas.org/type/file/7.0.1/sequence_file",
    #     "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/8.2.7/collection_protocol",
    #     "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/5.0.4/dissociation_protocol",
    #     "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/2.2.6/enrichment_protocol",
    #     "http://schema.dev.data.humancellatlas.org/type/protocol/sequencing/4.4.0/library_preparation_protocol",
    #     "http://schema.dev.data.humancellatlas.org/type/protocol/sequencing/9.0.3/sequencing_protocol",
    #     "http://schema.dev.data.humancellatlas.org/type/process/6.0.2/process"
    # ]

    spreadsheet_builder = SpreadsheetBuilder(output_file, hide_row)
    spreadsheet_builder.generate_workbook(tabs_template=args.yaml, schema_urls=all_schemas)
    spreadsheet_builder.save_workbook()

