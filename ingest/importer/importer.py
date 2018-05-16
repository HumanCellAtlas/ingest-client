import re

from ingest.importer.conversion import template_manager
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook


class WorkbookImporter:

    def __init__(self):
        self.worksheet_importer = WorksheetImporter()

    def do_import(self, workbook: IngestWorkbook):
        pre_ingest_json_map = {}

        tm = template_manager.build(workbook.get_schemas())
        for worksheet in workbook.importable_worksheets():
            json_list = self.worksheet_importer.do_import(worksheet, tm)
            concrete_entity = tm.get_concrete_entity_of_tab(worksheet.title)
            domain_entity = tm.get_domain_entity(concrete_entity)
            pre_ingest_json_map[domain_entity] = json_list
        return pre_ingest_json_map


class WorksheetImporter:

    def __init__(self):
        pass

    def do_import(self, worksheet, template:TemplateManager):
        concrete_entity = template.get_concrete_entity_of_tab(worksheet.title)

        rows_by_id = {}
        for row in self._get_data_rows(worksheet):
            node = template.create_template_node(worksheet)
            object_list_tracker = ObjectListTracker()
            row_id = None

            links = []

            for cell in row:
                # TODO preprocess headers so that cells can be converted without having to always
                # check the header
                header_name = self._get_header_name(cell, worksheet)

                cell_value = cell.value
                field_chain = self._get_field_chain(header_name)

                if cell_value is None:
                    continue

                if template.is_parent_field_multivalue(header_name):
                    object_list_tracker.track_value(field_chain, cell_value)
                    continue

                converter = template.get_converter(header_name)
                data = converter.convert(cell_value)

                node[field_chain] = data

                cell_concrete_entity = self._get_concrete_entity(template, header_name)

                if template.is_identifier_field(header_name):
                    if concrete_entity == cell_concrete_entity:
                        row_id = data
                    else: # this is a link column
                        link_domain_entity = template.get_domain_entity(concrete_entity=cell_concrete_entity)
                        links.append({
                            'entity': link_domain_entity,
                            'id': data
                        })

            object_list_fields = object_list_tracker.get_object_list_fields()
            for field_chain in object_list_fields:
                node[field_chain] = object_list_tracker.get_value_by_field(field_chain)

            if not row_id:
                title = worksheet.title
                row_index = row[0].row  # get first cell row index

                raise NoUniqueIdFoundError(title, row_index)

            rows_by_id[row_id] = {
                'content': node.as_dict(),
                'links': links
            }

        return rows_by_id

    def _get_data_rows(self, worksheet):
        return worksheet.iter_rows(row_offset=3, max_row=(worksheet.max_row - 3))

    def _get_header_name(self, cell, worksheet):
        header_coordinate = '%s1' % (cell.column)
        return worksheet[header_coordinate].value

    def _get_field_chain(self, header_name):
        match = re.search('(\w+\.){1}(?P<field_chain>.*)', header_name)
        return match.group('field_chain')

    def _get_concrete_entity(self, template_manager, header_name):
        return template_manager.get_concrete_entity_of_column(header_name)


class ObjectListTracker(object):

    def __init__(self):
        self.ontology_values = {}

    def _get_ontology_field(self, field_chain):
        match = re.search('(?P<field_chain>.*)(\.\w+)', field_chain)
        return match.group('field_chain')

    def _get_ontology_subfield(self, field_chain):
        match = re.search('(.*)\.(?P<field>\w+)', field_chain)
        return match.group('field')

    def track_value(self, header_name, value):
        ontology_subfield = self._get_ontology_subfield(header_name)
        ontology_field = self._get_ontology_field(header_name)

        if not self.ontology_values.get(ontology_field):
            self.ontology_values[ontology_field] = {}

        self.ontology_values[ontology_field][ontology_subfield] = value

    def get_object_list_fields(self):
        return self.ontology_values.keys()

    def get_value_by_field(self, field):
        return [self.ontology_values[field]]


class NoUniqueIdFoundError(Exception):
    def __init__(self, worksheet_title, row_index):
        message = f'No unique id found for row {row_index} in {worksheet_title} worksheet tab'
        super(NoUniqueIdFoundError, self).__init__(message)

        self.worksheet_title = worksheet_title
        self.row_index = row_index

