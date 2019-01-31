#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
import urllib
from argparse import ArgumentParser

__author__ = "jupp & hewgreen"
__license__ = "Apache 2.0"
__date__ = "31/01/2019"

from ingest.template import schema_template, tabs
from ingest.template.tabs import TabConfig
import xlsxwriter
import sys

DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"



class LinkedSheetBuilder:
    def __init__(self, output_file, hide_row=False, link_config=False, autofill_scale=1):

        self.workbook = xlsxwriter.Workbook(output_file)

        self.header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0', 'font_size': 12, 'valign': 'vcenter'})
        self.locked_format = self.workbook.add_format({'locked': True})
        # self.required_header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.desc_format = self.workbook.add_format({'font_color': '#808080', 'italic': True, 'text_wrap': True, 'font_size': 12, 'valign': 'top'})
        self.include_schemas_tab = False
        self.hidden_row = hide_row
        self.link_config = link_config
        self.autofill_scale = autofill_scale


    def _build(self, template):

        tabs = template.get_tabs_config()

        for tab in tabs.lookup("tabs"):

            for tab_name, detail in tab.items():

                if (self.link_config != False): # skip protocols and entities not in link_config
                    backbone = self.link_config[0]
                    len_check = [len(y) for y in backbone]
                    if all(x == len_check[0] for x in len_check):
                        backbone_entities = [list(y.keys())[0] for y in backbone]
                        self._protocol_linking(backbone_entities)
                    else:
                        print('Too many elements provided in backbone config. Aborting.')
                        sys.exit()

                    # todo project sub tabs contact etc need splitting out
                    always_add = ['project'] # todo NB hardcoded
                    if (tab_name not in backbone_entities) and (tab_name not in self.protocols_to_add) and (tab_name not in always_add):
                        continue # dont put the tab in if it isn't needed


                worksheet = self.workbook.add_worksheet(detail["display_name"])

                col_number = 0

                for cols in detail["columns"]:

                    if cols.split(".")[-1] == "text":
                            uf = self.get_user_friendly(template, cols.replace('.text', '')).upper()
                    else:
                        if cols+".text" not in detail["columns"]:
                            uf = self.get_user_friendly(template, cols).upper()
                    if cols.split(".")[-1] == "text":
                        desc = self._get_value_for_column(template, cols.replace('.text', ''), "description")
                        if desc == "":
                            desc = self._get_value_for_column(template, cols, "description")
                    else:
                        if cols+".text" not in detail["columns"]:
                            desc = self._get_value_for_column(template, cols, "description")
                    if cols.split(".")[-1] == "text":
                        required = bool(self._get_value_for_column(template, cols.replace('.text', ''), "required"))
                    else:
                        if cols+".text" not in detail["columns"]:
                            required = bool(self._get_value_for_column(template, cols, "required"))
                    if cols.split(".")[-1] == "text":
                        example_text = self._get_value_for_column(template, cols.replace('.text', ''), "example")
                        if example_text == "":
                            example_text = self._get_value_for_column(template, cols, "example")
                    else:
                        if cols+".text" not in detail["columns"]:
                            example_text = self._get_value_for_column(template, cols, "example")
                    if cols.split(".")[-1] == "text":
                        guidelines = self._get_value_for_column(template, cols.replace('.text', ''), "guidelines")
                        if guidelines == "":
                            guidelines = self._get_value_for_column(template, cols, "guidelines")
                    else:
                        if cols+".text" not in detail["columns"]:
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
                        worksheet.write(2, col_number, guidelines , self.desc_format)

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

                    col_number+=1
                    # worksheet.merge_range(first_col=0, first_row=4, last_col= len(detail["columns"]), last_row=4, cell_format= self.header_format, data="FILL OUT INFORMATION BELOW THIS ROW")

                if self.link_config != False: # after normal cols added to tab add linking cols
                    self._make_col_name_mapping(template)  # makes lookup dict for uf tab names
                    self._add_link_cols(tab_name, col_number, worksheet, hf, backbone_entities)



        if self.include_schemas_tab:
            self._write_schemas(template.get_schema_urls())

        return self

    def _add_link_cols(self, tab_name, col_number, worksheet, hf, backbone_entities):

        #given the tab name work out what entity links need to be added

        _link_to_tab = [backbone_entities[s - 1] for s in [i for i, e in enumerate(backbone_entities) if e == tab_name]]


        # add link columns
        for link_to_tab in _link_to_tab:
            display_name = self.col_name_mapping.get(link_to_tab)[0]
            prog_name = self.col_name_mapping.get(link_to_tab)[1]
            uf = str('DERIVED FROM {}'.format(display_name.upper()))
            desc = str('Enter biomaterial ID from "{}" tab that this entity was derived from.'.format(display_name))
            # todo make example, guidelines and description fancier
            col_number = self._write_column_head(worksheet, col_number, uf, hf, desc, prog_name)


        # add protocol columns
        # all_to_add = []
        reverse_dict = {}
        for k,v in self.protocols_to_add.items():
            # all_to_add += v
            for entity in v:
                if entity in reverse_dict:
                    reverse_dict[entity] = reverse_dict.get(entity) + [k]
                else:
                    reverse_dict[entity] = [k]


        if tab_name in reverse_dict:
            add_these = reverse_dict.get(tab_name)
            for protocol in add_these:
                display_name = self.col_name_mapping.get(protocol)[0]
                prog_name = self.col_name_mapping.get(protocol)[1]
                uf = str('ID OF {} USED'.format(display_name.upper()))
                desc = str('Enter protocol ID from "{}" tab that this entity was derrived from.'.format(display_name))
                col_number = self._write_column_head(worksheet, col_number, uf, hf, desc, prog_name)






    def _write_column_head(self, worksheet, col_number, uf, hf, desc, prog_name):
        worksheet.write(0, col_number, uf, hf)  # user friendly name
        worksheet.write(1, col_number, desc, self.desc_format)  # description
        # worksheet.write(2, col_number, ???, self.desc_format) # example
        worksheet.write(3, col_number, prog_name, self.locked_format)  # programatic name
        worksheet.write(4, col_number, '', hf)  # blank column
        col_number += 1
        return col_number


    def _protocol_linking(self, backbone_entities):
        protocol_pairings = self.link_config[1]
        # protocols_to_add = []
        protocols_to_add = {}

        index_counter = 0
        for entity in backbone_entities:
            index_counter += 1
            the_source = entity
            try:
                the_output = backbone_entities[index_counter]
            except IndexError:
                self.protocols_to_add = protocols_to_add
                return
            for key, value in protocol_pairings.items():
                protocol_prog_name = key
                for pairing in value:
                    if (pairing.get('source') == the_source) and (pairing.get('output') == the_output):
                        # protocols_to_add.append(protocol_prog_name)
                        if protocol_prog_name in protocols_to_add:
                            protocols_to_add[protocol_prog_name] = protocols_to_add.get(protocol_prog_name).append(the_output)
                        else:
                            protocols_to_add[protocol_prog_name] = [the_output]










    def _make_col_name_mapping(self, template):
        col_name_mapping = {}
        for entity_type in template._template.get('tabs'):
            for key, value in entity_type.items():
                display_name = value.get('display_name')
                for col in value.get('columns'):
                    dot_parse = col.split('.')
                    if (len(dot_parse) == 3) and ((dot_parse[2] == 'protocol_id') or (dot_parse[2] == 'biomaterial_id')) and (dot_parse[1].endswith('_core')): #todo WARNING verging on hard coded
                        prog_name = col

                col_name_mapping[key] = [display_name, prog_name]
        self.col_name_mapping = col_name_mapping


    def _get_value_for_column(self, template, col_name, property):
        try:
            uf = str(template.lookup(col_name + "."+property)) if template.lookup(col_name + "."+property) else ""
            return uf
        except:
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
        except:
            return key


    def _write_schemas(self, schema_urls):
        worksheet = self.workbook.add_worksheet("Schemas")
        worksheet.write(0, 0, "Schemas")
        for index, url in enumerate(schema_urls):
            worksheet.write(index + 1, 0, url)

    # unmodified by hewgreen


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


    def save_workbook(self):
        self.workbook.close()





if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-y", "--yaml", dest="yaml",
                      help="The YAML file from which to generate the spreadsheet")
    parser.add_argument("-o", "--output", dest="output",
                      help="Name of the output spreadsheet")
    parser.add_argument("-u", "--url", dest="url",
                      help="Optional ingest API URL - if not default (prod)")
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

    hide_row = False

    if args.hidden_row:
        hide_row = True

    all_schemas = schema_template.SchemaTemplate(ingest_url).get_schema_urls()

    spreadsheet_builder = SpreadsheetBuilder(output_file, hide_row)
    spreadsheet_builder.generate_workbook(tabs_template=args.yaml, schema_urls=all_schemas)
    spreadsheet_builder.save_workbook()

