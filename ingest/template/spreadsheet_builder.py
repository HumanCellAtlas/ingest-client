"""
Given a tabs template and list of schema URLs, will output a spreadsheet in Xls format
"""
import urllib
from argparse import ArgumentParser

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

from ingest.template import schema_template, tabs
from ingest.template.tabs import TabConfig
import xlsxwriter


DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"



class SpreadsheetBuilder:
    def __init__(self, output_file, biomaterial_links, protocol_links, hide_row=False, fill_examples=False):

        self.workbook = xlsxwriter.Workbook(output_file)

        self.header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0', 'font_size': 12, 'valign': 'vcenter'})
        self.locked_format = self.workbook.add_format({'locked': True})
        # self.required_header_format = self.workbook.add_format({'bold': True, 'bg_color': '#D0D0D0'})
        self.desc_format = self.workbook.add_format({'font_color': '#808080', 'italic': True, 'text_wrap': True, 'font_size': 12, 'valign': 'top'})
        self.include_schemas_tab = False
        self.hidden_row = hide_row
        self.fill_examples = fill_examples
        self.backbone_links = biomaterial_links
        self.protocol_links = protocol_links


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

    def save_workbook(self):
        self.workbook.close()

    def _write_schemas(self, schema_urls):
        worksheet = self.workbook.add_worksheet("Schemas")
        worksheet.write(0, 0, "Schemas")
        for index, url in enumerate(schema_urls):
            worksheet.write(index + 1, 0, url)

    def _tab_build(self, detail, template):

        worksheet = self.workbook.add_worksheet(detail["display_name"])

        col_number = 0

        for cols in detail["columns"]:

            if cols.split(".")[-1] == "text":
                uf = self.get_user_friendly(template, cols.replace('.text', '')).upper()
            else:
                uf = self.get_user_friendly(template, cols).upper()
            if cols.split(".")[-1] == "text":
                desc = self._get_value_for_column(template, cols.replace('.text', ''), "description")
                if desc == "":
                    desc = self._get_value_for_column(template, cols, "description")
            else:
                desc = self._get_value_for_column(template, cols, "description")
            if cols.split(".")[-1] == "text":
                required = bool(self._get_value_for_column(template, cols.replace('.text', ''), "required"))
            else:
                required = bool(self._get_value_for_column(template, cols, "required"))
            if cols.split(".")[-1] == "text":
                example_text = self._get_value_for_column(template, cols.replace('.text', ''), "example")
                if example_text == "":
                    example_text = self._get_value_for_column(template, cols, "example")
            else:
                example_text = self._get_value_for_column(template, cols, "example")
            if cols.split(".")[-1] == "text":
                guidelines = self._get_value_for_column(template, cols.replace('.text', ''), "guidelines")
                if guidelines == "":
                    guidelines = self._get_value_for_column(template, cols, "guidelines")
            else:
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

            if self.fill_examples:
                double_prefix = 'Should be one of: '
                metadata_fs = ';'
                if example_text.startswith(double_prefix):
                    example_text_ =  example_text[len(double_prefix):].split(',')[0] # TODO Hard coding assumptions alert!
                elif metadata_fs in example_text:
                    example_text_ = example_text.split(metadata_fs)[0]
                else:
                    example_text_ = example_text

                worksheet.write(5, col_number, example_text_, self.locked_format)

            # TODO noticed donor_organism.genus_species is present. Should this be donor_organism.genus_species.text?

            col_number += 1

            # worksheet.merge_range(first_col=0, first_row=4, last_col= len(detail["columns"]), last_row=4, cell_format= self.header_format, data="FILL OUT INFORMATION BELOW THIS ROW")
        return (col_number, worksheet) # objects needed if linking is required

    def _add_links(self, tab_name, worksheet, col_number, hf, entity_uf):

        link_to = self.backbone_links.get(tab_name)
        if isinstance(link_to, str):
            link_to = [link_to]

        protocols_links_to_add = self.include_protocol_links(tab_name)
        # TODO make code add the links to protocols here
        if len(protocols_links_to_add) > 0:
            for protocol in protocols_links_to_add:
                uf = self.entity_uf.get(protocol).get('user_friendly').upper()
                desc = str('Enter protocol ID from {} tab that this entity was derrived from.'.format(uf))
                key = protocol + '.protocol_core.protocol_id'

                worksheet.write(0, col_number, uf, hf)  # write user friendly
                worksheet.write(1, col_number, desc, self.desc_format)  # write description
                worksheet.write(3, col_number, key, self.locked_format)
                worksheet.write(4, col_number, '', hf)
                col_number += 1

        for link in link_to:
            display_name = entity_uf.get(link).get('user_friendly').upper()
            uf = str('DERIVED FROM {}'.format(display_name))
            desc = str('Enter biomaterial ID from {} tab that this entity was derrived from.'.format(display_name))
            entity_type = entity_uf.get(link).get('entity_type')

            if entity_type == 'biomaterial':
                key = link + '.biomaterial_core.biomaterial_id'
            elif entity_type =='file':
                key = link + '.file_core.file_id'

            worksheet.write(0, col_number, uf, hf)  # write user friendly
            worksheet.write(1, col_number, desc, self.desc_format)  # write description
            worksheet.write(3, col_number, key, self.locked_format)
            worksheet.write(4, col_number, '', hf)
            col_number += 1
            # print('Make link from {} to {}'.format(tab_name, link_to))

    def _build(self, template):

        tabs = template.get_tabs_config()
        protocols_to_add = self.include_protocols()
        entity_uf = self.get_uf_names(tabs)

        for tab in tabs.lookup("tabs"):

            # display_name = next(iter(tab.values())).get('display_name')

            for tab_name, detail in tab.items():
                # metadata = tabs._dic.get("meta_data_properties")
                # domain_entity = metadata.get(tab_name).get('schema').get('domain_entity')

                entity_type = entity_uf.get(tab_name).get('entity_type')

                if self.backbone_links is not False:
                    entities = set()
                    for entity1, entity2 in self.backbone_links.items():
                        entities.add(entity1)
                        entities.add(entity2)
                    self.entities = entities


                    if entity_type == 'biomaterial'or entity_type == 'file':
                        if tab_name in entities:  # ignored if not provided
                            if tab_name in self.backbone_links.keys():
                                no_linking = self._tab_build(detail, template)
                                col_number = no_linking[0]
                                worksheet = no_linking[1]
                                hf = self.header_format
                                # print('Adding links {}'.format(tab_name))
                                self._add_links(tab_name, worksheet, col_number, hf, entity_uf)
                            else:
                                # print('NOT adding links {}'.format(tab_name))
                                self._tab_build(detail, template) # adds top level entity with no link
                        else:
                            pass # skips unlinked biomaterial tabs

                    if entity_type.startswith('protocol'):
                        if tab_name in protocols_to_add:
                            self._tab_build(detail, template)
                        else:
                            pass # skip protocols that have never been used between backbone entities provided

                else:
                    self._tab_build(detail, template)


        if self.include_schemas_tab:
            self._write_schemas(template.get_schema_urls())

        return self

    def include_protocols(self): # generate a list of protocol tabs that should be included given a backbone and protocol linking list

        protocols_to_add = set()
        for output, source in self.backbone_links.items():
            for protocol, context_list in self.protocol_links.items():
                for context in context_list:
                    context_source = context.get('source')
                    context_output = context.get('output')
                    if output == context_output and source == context_source:
                        protocols_to_add.add(protocol)
        return protocols_to_add

    def include_protocol_links(self, tab_name):
        protocols_links_to_add = set()
        for protocol, loc in self.protocol_links.items():
            for pair in loc:
                if pair.get('output') == tab_name:
                    if pair.get('source') in self.entities:
                        protocols_links_to_add.add(protocol)
        return protocols_links_to_add



    def get_uf_names(self, tabs):
        metadata = tabs._dic.get("meta_data_properties")

        entity_uf = {}
        for tab in tabs.lookup("tabs"):
            name = next(iter(tab.keys()))
            uf_name = next(iter(tab.values())).get('display_name')

            domain_entity = metadata.get(name).get('schema').get('domain_entity')

            entity_uf[name] = {'user_friendly' : uf_name, 'entity_type' : domain_entity}
        self.entity_uf = entity_uf



        return entity_uf








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
    parser.add_argument("-b", "--biomaterial_linking", dest="biomaterial_linking",
                        help="Optional pointer to link backbone to add linking columns")
    parser.add_argument("-p", "--protocol_linking", dest="protocol_linking",
                        help="Optional pointer to link protocols")
    parser.add_argument("-r", "--fill_examples", action="store_true",
                        help="Binary flag - if set, where possible examples will be used as data")
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

    fill_examples = False

    if args.fill_examples:
        fill_examples = True

    if not args.biomaterial_linking:
        biomaterial_links = False
    else:
        biomaterial_links = args.biomaterial_linking

    if not args.protocol_linking:
        protocol_links = False
    else:
        protocol_links = args.protocol_linking




    all_schemas = schema_template.SchemaTemplate(ingest_url).get_schema_urls()

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

    spreadsheet_builder = SpreadsheetBuilder(output_file, hide_row, biomaterial_links, protocol_links, fill_examples)
    spreadsheet_builder.generate_workbook(tabs_template=args.yaml, schema_urls=all_schemas)
    spreadsheet_builder.save_workbook()