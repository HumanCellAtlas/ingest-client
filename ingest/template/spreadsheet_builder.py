#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

from ingest.template import schema_template, tabs
from ingest.template.tabs import TabConfig
import xlsxwriter


class SpreadsheetBuilder:
    def __init__(self, output_file):

        self.workbook = xlsxwriter.Workbook(output_file)

        self.header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.required_header_format = self.workbook.add_format({'bold': True, 'bg_color': 'yellow'})
        self.desc_format = self.workbook.add_format({'font_color': 'light-grey', 'italic': True, 'text_wrap': True})

    def generate_workbook(self, tabs_template=None, schema_urls=None):

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



    def _build(self, template):

        tabs = template.get_tabs_config()

        for tab in tabs.lookup("tabs"):

            for tab_name, detail in tab.items():

                worksheet = self.workbook.add_worksheet(detail["display_name"])

                col_number = 0


                for cols in detail["columns"]:
                    uf = self.get_user_friendly(template, cols)
                    desc = self._get_value_for_column(template, cols, "description")
                    required = bool(self._get_value_for_column(template, cols, "required"))
                    example_text = self._get_value_for_column(template, cols, "example")

                    hf = self.header_format
                    if required:
                        hf= self.required_header_format

                    # set the description
                    worksheet.write(0, col_number, desc, self.desc_format)


                    # set the user friendly name
                    worksheet.write(1, col_number, uf, hf)
                    worksheet.set_column(col_number, col_number, len(uf))


                    worksheet.write(2, col_number, example_text)

                    # set the key
                    worksheet.write(3, col_number, cols)

                    worksheet.write(4, 0, "Add your data below this line", self.header_format)

                    col_number+=1


        worksheet = self.workbook.add_worksheet("Schemas")
        worksheet.write(0,0, "Schemas")
        for index, url in enumerate(template.get_schema_urls()):
            worksheet.write(index+1, 0, url)


        return self


