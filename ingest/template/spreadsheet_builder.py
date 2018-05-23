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

def generate_spreadsheet(outputfile, tabs_template=None, schema_urls=None):
    template = None

    if tabs_template:

        tabs_parser = TabConfig()
        tabs = tabs_parser.load(tabs_template)
        template = schema_template.SchemaTemplate(schema_urls, tab_config=tabs)
    else:
        template = schema_template.SchemaTemplate(schema_urls)


    _build(outputfile, template, schema_urls)


def _build(outputfile, template, schema_urls):

    tabs = template.get_tabs_config()

    workbook = xlsxwriter.Workbook(outputfile)

    header_format = workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
    required_header_format = workbook.add_format({'bold': True, 'bg_color': 'yellow'})
    desc_format = workbook.add_format({'font_color':'light-grey', 'italic': True, 'text_wrap':True})

    for tab in tabs.lookup("tabs"):

        for tab_name, detail in tab.items():

            worksheet = workbook.add_worksheet(detail["display_name"])

            col_number = 0


            for cols in detail["columns"]:
                uf = cols

                try:
                    uf = template.lookup(cols+".user_friendly") if template.lookup(cols+".user_friendly") else cols
                except :
                    print ("No property for "+cols)

                desc = ""
                try:
                    desc = template.lookup(cols+".description") if template.lookup(cols+".description") else ""
                except:
                    print ("No for "+cols)

                required = False
                try:
                    required = template.lookup(cols+".required") if template.lookup(cols+".required") else False
                except:
                    print ("No property for "+cols)

                # set the example
                example_text = ""
                try:
                    example_text = "e.g. " + str(template.lookup(cols + ".example")) if template.lookup(cols + ".example") else ""
                except:
                    print ("No property for "+cols)


                hf = header_format
                if required:
                    hf= required_header_format

                # set the description
                worksheet.write(0, col_number, desc, desc_format)


                # set the user friendly name
                worksheet.write(1, col_number, uf, hf)
                worksheet.set_column(col_number, col_number, len(uf))


                worksheet.write(2, col_number, example_text)

                # set the key
                worksheet.write(3, col_number, cols)

                worksheet.write(4, 0, "Add your data below this line", header_format)

                col_number+=1


    worksheet = workbook.add_worksheet("Schemas")
    worksheet.write(0,0, "Schemas")
    for index, url in enumerate(schema_urls):
        worksheet.write(index+1, 0, url)

    workbook.close()


