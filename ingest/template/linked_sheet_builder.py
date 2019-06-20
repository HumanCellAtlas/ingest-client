#!/usr/bin/env python
"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
from argparse import ArgumentParser

__author__ = "jupp & hewgreen"
__license__ = "Apache 2.0"
__date__ = "31/01/2019"

from ingest.template import schema_template
from ingest.template.tabs import TabConfig
from .spreadsheet_builder import SpreadsheetBuilder
import xlsxwriter
import sys

DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"


class LinkedSheetBuilder:
    def __init__(self, output_file, hide_row=False, link_config=False, autofill_scale=0):

        self.workbook = xlsxwriter.Workbook(output_file)

        self.header_format = self.workbook.add_format(
            {'bold': True, 'bg_color': '#D0D0D0', 'font_size': 12, 'valign': 'vcenter'})
        self.locked_format = self.workbook.add_format({'locked': True})
        # self.required_header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.desc_format = self.workbook.add_format(
            {'font_color': '#808080', 'italic': True, 'text_wrap': True, 'font_size': 12, 'valign': 'top'})
        self.include_schemas_tab = False
        self.hidden_row = hide_row
        self.link_config = link_config
        self.autofill_scale = autofill_scale

    def _build(self, template):

        tabs = template.get_tabs_config()
        if (self.link_config is not False) and (self.autofill_scale != 0):  # some precalculation for whole sheet
            self._value_linking()

        for tab in tabs.lookup("tabs"):

            for tab_name, detail in tab.items():

                if self.link_config is not False:  # skip protocols and entities not in link_config
                    backbone = self.link_config[0]
                    len_check = [len(y) for y in backbone]
                    if all(x == len_check[0] for x in len_check):
                        self.backbone_entities = [list(y.keys())[0] for y in backbone]
                        self._protocol_linking(self.backbone_entities)
                    else:
                        print('Too many elements provided in backbone config. Aborting.')
                        sys.exit()

                    # todo project sub tabs contact etc need splitting out
                    always_add = ['project']  # todo NB hardcoded
                    if (tab_name not in self.backbone_entities) and (tab_name not in self.protocols_to_add) and (
                            tab_name not in always_add):
                        continue  # dont put the tab in if it isn't needed

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

                    # ADD EXAMPLES HERE
                    if self.autofill_scale > 0:  # flag for adding example info
                        self._fill_examples_from_schema(example_text, worksheet, cols, col_number, tab_name)

                    col_number += 1
                    # worksheet.merge_range(first_col=0, first_row=4, last_col= len(detail["columns"]), last_row=4,
                    # cell_format= self.header_format, data="FILL OUT INFORMATION BELOW THIS ROW")

                if self.link_config is not False:  # after normal cols added to tab add linking cols

                    self._make_col_name_mapping(template)  # makes lookup dict for uf tab names
                    self._add_link_cols(tab_name, col_number, worksheet, hf, self.backbone_entities)

                    # todo add process column if on seq tab

        if self.include_schemas_tab:
            self._write_schemas(template.get_schema_urls())

        return self

    def _fill_examples_from_schema(self, example_text, worksheet, cols, col_number, tab_name):
        double_prefix = 'Should be one of: '
        metadata_fs = ';'
        if example_text.startswith(double_prefix):
            example_text_ = example_text[len(double_prefix):].split(',')[0]  # TODO hard coded assumption?
        elif metadata_fs in example_text:
            example_text_ = example_text.split(metadata_fs)[0]
        else:
            example_text_ = example_text

        prog_name = cols.split('.')

        one_liner_tabs = ['project']
        if (tab_name.endswith('protocol')) or (tab_name in one_liner_tabs):
            row_range_fill = 1
        else:
            row_range_fill = self.tab_multiplier.get(tab_name).get('y') * self.autofill_scale

        for x in range(row_range_fill):
            row_no = 5 + x

            # the first ID field
            if (len(prog_name) == 3) and (prog_name[2].endswith('_id') and (
                    prog_name[1].endswith('_core') and (col_number == 0))):  # TODO hard coded assumption?
                example_text_ = prog_name[0] + '_' + str(x)  # TODO add row counter here to support multiple row fills
            # the name field
            if (len(prog_name) == 3) and (
                    prog_name[2].endswith('_name') and (prog_name[1].endswith('_core'))):  # TODO hard coded assumption?
                example_text_ = prog_name[0] + '_' + str(x)
            # the description field
            if (len(prog_name) == 3) and (prog_name[2].endswith('_description') and (
                    prog_name[1].endswith('_core'))):  # TODO hard coded assumption?
                example_text_ = 'This is a description of ' + prog_name[0]

            worksheet.write(row_no, col_number, example_text_, self.locked_format)

            # project and protocol doesn't need these multiple rows

    def _value_linking(self):

        # # work out from backbone list and autofill_scale how many id values to fill
        # backbone = self.link_config[0]
        # multiplier = []
        # for entity_item in backbone:
        #     for entity,bundle_multiplier in entity_item.items(): # should only ever be one item checked elsewhere
        #         total_multiplier = bundle_multiplier * self.autofill_scale
        #         multiplier_ = {}
        #         multiplier_[entity] = total_multiplier
        #         multiplier.append(multiplier_) # list to maintain backbone ordering
        #
        # self.multiplier = multiplier

        backbone = self.link_config[0]
        pre_multiplier = []
        for entity in backbone:
            entity_entry = {}
            prev_entity = None
            for tab_name, y in entity.items():
                entity_entry['tab_name'] = tab_name
                entity_entry['y'] = y
                if len(pre_multiplier):
                    entity_entry['text'] = prev_entity
                else:
                    entity_entry['text'] = None
                prev_entity = tab_name
                pre_multiplier.append(entity_entry)

        post_multiplier = []
        backbone_index = 0
        for sheet in pre_multiplier:
            # print('I am on sheet: {}'.format(sheet.get('tab_name')))
            mid_multiplier = []
            counter = 0
            if backbone_index != 0:
                y = sheet.get('y') * self.autofill_scale
                prev_y = pre_multiplier[backbone_index - 1].get('y') * self.autofill_scale
                if y >= prev_y:
                    print_count = int(y / prev_y)
                    # print('y {}'.format(y))
                    # print('prev_y {}'.format(prev_y))
                    # print('print_count {}'.format(print_count))
                    for row in range(prev_y):
                        for row_ in range(print_count):
                            to_print = sheet.get('text') + '_' + str(counter)
                            mid_multiplier.append(to_print)
                        counter += 1
                else:
                    print_count = int(prev_y / y)
                    # print('y {}'.format(y))
                    # print('prev_y {}'.format(prev_y))
                    # print('print_count {}'.format(print_count))
                    counter = 0
                    for row in range(y):
                        to_print = ''

                        for row_ in range(print_count):
                            to_print += sheet.get('text') + '_' + str(counter) + '||'
                            counter += 1
                        mid_multiplier.append(to_print[:-2])
                sheet['pre_comb_linking'] = mid_multiplier
            post_multiplier.append(sheet)

            backbone_index += 1

        uniques = []
        tab_multiplier = {}
        for entity in post_multiplier:
            if entity.get('tab_name') not in uniques:
                uniques.append(entity.get('tab_name'))
                tab_multiplier[entity.get('tab_name')] = entity  # add pop tab name
            else:
                prev_entity = tab_multiplier.get(entity.get('tab_name'))
                prev_entity['y'] = entity.get('y') + prev_entity.get('y')
                prev_entity['pre_comb_linking'] = prev_entity.get('pre_comb_linking') + entity.get('pre_comb_linking')

        self.tab_multiplier = tab_multiplier

    def _add_link_cols(self, tab_name, col_number, worksheet, hf, backbone_entities):

        # given the tab name work out what entity links need to be added

        _link_to_tab = []
        index_list = [i for i, e in enumerate(backbone_entities) if e == tab_name]
        for x in index_list:
            if x > 0:
                _link_to_tab.append(backbone_entities[x - 1])

        # add link columns
        for link_to_tab in _link_to_tab:
            display_name = self.col_name_mapping.get(link_to_tab)[0]
            prog_name = self.col_name_mapping.get(link_to_tab)[1]
            uf = str('DERIVED FROM {}'.format(display_name.upper()))
            desc = str('Enter biomaterial ID from "{}" tab that this entity was derived from.'.format(display_name))

            col_number = self._write_column_head(worksheet, col_number, uf, hf, desc, prog_name, tab_name)

        # add protocol columns
        # all_to_add = []
        reverse_dict = {}
        for k, v in self.protocols_to_add.items():
            # all_to_add += v
            try:
                for entity in v:
                    if entity in reverse_dict:
                        reverse_dict[entity] = reverse_dict.get(entity) + [k]
                    else:
                        reverse_dict[entity] = [k]
            except TypeError:
                continue

        if tab_name in reverse_dict:
            add_these = reverse_dict.get(tab_name)
            for protocol in add_these:
                display_name = self.col_name_mapping.get(protocol)[0]
                prog_name = self.col_name_mapping.get(protocol)[1]
                uf = str('ID OF {} USED'.format(display_name.upper()))
                desc = str('Enter protocol ID from "{}" tab that this entity was derrived from.'.format(display_name))
                col_number = self._write_column_head(worksheet, col_number, uf, hf, desc, prog_name, tab_name)

    def _write_column_head(self, worksheet, col_number, uf, hf, desc, prog_name, tab_name):
        worksheet.write(0, col_number, uf, hf)  # user friendly name
        worksheet.write(1, col_number, desc, self.desc_format)  # description
        # worksheet.write(2, col_number, ???, self.desc_format) # example
        worksheet.write(3, col_number, prog_name, self.locked_format)  # programatic name
        worksheet.write(4, col_number, '', hf)  # blank column

        # todo loop to fill in values for links (this func only fill link back
        # AKA ADD EXAMPLES

        if (self.link_config is not False) and (self.autofill_scale != 0):
            prog_name_list = prog_name.split('.')
            link_fill = self.tab_multiplier.get(tab_name).get('pre_comb_linking')
            row_no = 5
            if (len(prog_name_list) == 3) and (prog_name_list[2].endswith('biomaterial_id') and (
                    prog_name_list[1].endswith('_core'))):
                for x in link_fill:
                    worksheet.write(row_no, col_number, x, self.locked_format)
                    row_no += 1
            else:
                fill = prog_name_list[0] + str('_0')
                for x in range(len(link_fill)):
                    worksheet.write(row_no, col_number, fill, self.locked_format)
                    row_no += 1
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
                            protocols_to_add[protocol_prog_name] = protocols_to_add.get(protocol_prog_name).append(
                                the_output)
                        else:
                            protocols_to_add[protocol_prog_name] = [the_output]

    def _make_col_name_mapping(self, template):
        col_name_mapping = {}
        for entity_type in template._template.get('tabs'):
            for key, value in entity_type.items():
                display_name = value.get('display_name')
                for col in value.get('columns'):
                    dot_parse = col.split('.')
                    if (len(dot_parse) == 3) and (
                            (dot_parse[2] == 'protocol_id') or (dot_parse[2] == 'biomaterial_id')) and (
                            dot_parse[1].endswith('_core')):  # todo WARNING verging on hard coded
                        prog_name = col

                col_name_mapping[key] = [display_name, prog_name]
        self.col_name_mapping = col_name_mapping

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
