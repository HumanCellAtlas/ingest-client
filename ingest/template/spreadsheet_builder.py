#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
import urllib
from optparse import OptionParser

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

from ingest.template import schema_template, tabs
from ingest.template.tabs import TabConfig
import xlsxwriter


DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"



class SpreadsheetBuilder:
    def __init__(self, output_file):

        self.workbook = xlsxwriter.Workbook(output_file)

        self.header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.locked_format = self.workbook.add_format({'locked': True})
        # self.required_header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.desc_format = self.workbook.add_format({'font_color': '#808080', 'italic': True, 'text_wrap': True})
        self.include_schemas_tab = False

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
            uf = str(template.lookup(col_name + "."+property)) if template.lookup(col_name + "."+property) else ""
            return uf
        except:
            print("No property for " + col_name)
            return ""

    def get_user_friendly(self, template, col_name):
        key = col_name + ".user_friendly"
        try:
            uf = str(template.lookup(key)) if template.lookup(key) else col_name
            return uf
        except:
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

                    uf = self.get_user_friendly(template, cols).upper()
                    desc = self._get_value_for_column(template, cols, "description")
                    # required = bool(self._get_value_for_column(template, cols, "required"))
                    example_text = self._get_value_for_column(template, cols, "example")

                    hf = self.header_format
                    # if required:
                    #     hf= self.required_header_format



                    # set the user friendly name
                    worksheet.write(0, col_number, uf, hf)

                    if len(uf) < 15:
                        col_w = 15
                    else:
                        col_w = len(uf)

                    worksheet.set_column(col_number, col_number, col_w)

                    # set the description
                    worksheet.write(1, col_number, desc, self.desc_format)


                    # write example
                    worksheet.write(2, col_number, example_text, self.desc_format)


                    # set the key
                    worksheet.write(3, col_number, cols, self.locked_format)

                    worksheet.set_row(3, None, None, {'hidden': True})



                    col_number+=1

                worksheet.merge_range(first_col=0, first_row=4, last_col= len(detail["columns"]), last_row=4, cell_format= self.header_format, data="FILL OUT INFORMATION BELOW THIS ROW")

        if self.include_schemas_tab:
            self._write_schemas(template.get_schema_urls())

        return self




if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-y", "--yaml", dest="yaml",
                      help="The YAML file from which to generate the spreadsheet")
    parser.add_option("-o", "--output", dest="output",
                      help="Name of the output spreadsheet")
    parser.add_option("-u", "--url", dest="url",
                      help="Optional ingest API URL - if not default (prod)")
    (options, args) = parser.parse_args()

    if not options.output:
        output_file = "template_spreadsheet.xlsx"
    else:
        output_file = options.output

    if not options.url:
        ingest_url = DEFAULT_INGEST_URL
    else:
        ingest_url = options.url
    schemas_url = ingest_url + DEFAULT_SCHEMAS_ENDPOINT

    all_schemas = schema_template.SchemaTemplate(ingest_url).get_schema_urls()

    spreadsheet_builder = SpreadsheetBuilder(output_file)
    spreadsheet_builder.generate_workbook(tabs_template=options.yaml, schema_urls=all_schemas)
    spreadsheet_builder.save_workbook()

