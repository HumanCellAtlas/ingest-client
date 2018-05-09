#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

from ingest.template import schematemplate, template_tabs
import xlsxwriter

def generate_spreadsheet(outputfile, tabs_template, schema_urls):

    tabs_parser = template_tabs.TabParser()
    tabs = tabs_parser.load_template(tabs_template)

    _build(outputfile, tabs, schema_urls)


def _build(outputfile, tabs, schema_urls):

    template = schematemplate.get_template_from_schemas_by_url(schema_urls)

    workbook = xlsxwriter.Workbook(outputfile)

    header_format = workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
    required_header_format = workbook.add_format({'bold': True, 'bg_color': 'yellow'})
    desc_format = workbook.add_format({'font_color':'light-grey', 'italic': True, 'text_wrap':True})

    for tab_info in tabs.lookup("tabs"):

        worksheet = workbook.add_worksheet(tabs.lookup("tabs."+tab_info+".display_name"))

        col_number = 0
        for cols in tabs.lookup("tabs."+tab_info+".columns"):
            uf = template.lookup(cols+".user_friendly")
            if not uf:
                uf = cols
            desc = template.lookup(cols+".description")
            if not desc:
                desc = ""

            required = template.lookup(cols+".required")
            hf = header_format
            if required:
                hf= required_header_format


            worksheet.write(0, col_number, uf, hf)
            worksheet.set_column(col_number, col_number, len(uf))

            worksheet.write(1, col_number, desc, desc_format)
            col_number+=1

        print (tab_info)

    workbook.close()


