import re
import openpyxl

from ingest.importer.conversion import template_manager, conversion_strategy
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook
from ingest.importer.submission import IngestSubmitter

from ingest.api.ingestapi import IngestApi


class IngestImporter:

    def __init__(self, ingest_api):
        self.ingest_api = ingest_api

    def import_spreadsheet(self, file_path, submission_url, dry_run=False):
        workbook = openpyxl.load_workbook(filename=file_path)
        ingest_workbook = IngestWorkbook(workbook)
        schemas = ingest_workbook.get_schemas()

        template_mgr = template_manager.build(schemas)
        workbook_importer = WorkbookImporter(template_mgr)
        spreadsheet_json = workbook_importer.do_import(ingest_workbook)

        submission = None
        if not dry_run:
            submitter = IngestSubmitter(self.ingest_api, template_mgr)
            submission = submitter.submit(spreadsheet_json, submission_url)
            print(f'Submission in {submission_url} is done!')

        return submission


class WorkbookImporter:

    def __init__(self, template_mgr):
        self.worksheet_importer = WorksheetImporter()
        self.template_mgr = template_mgr

    def do_import(self, workbook: IngestWorkbook):
        pre_ingest_json_map = {}

        for worksheet in workbook.importable_worksheets():
            entities_dict = self.worksheet_importer.do_import(worksheet, self.template_mgr)
            concrete_entity = self.template_mgr.get_concrete_entity_of_tab(worksheet.title)

            # TODO what if the tab is not a valid entity?
            if concrete_entity is None:
                print(f'{worksheet.title} is not a valid tab name.')
                continue

            domain_entity = self.template_mgr.get_domain_entity(concrete_entity)

            if pre_ingest_json_map.get(domain_entity) is None:
                pre_ingest_json_map[domain_entity] = {}

            pre_ingest_json_map[domain_entity].update(entities_dict)
        return pre_ingest_json_map


class WorksheetImporter:
    KEY_HEADER_ROW_IDX = 4
    USER_FRIENDLY_HEADER_ROW_IDX = 2
    START_ROW_IDX = 5

    def __init__(self):
        pass

    def do_import(self, worksheet, template: TemplateManager):
        records = self._import_records(worksheet, template)
        entity_type = template.get_concrete_entity_of_tab(worksheet.title)
        pre_ingest_entry = {entity_type: records}
        return pre_ingest_entry

    def _import_records(self, worksheet, template: TemplateManager):
        records = {}
        row_template = template.create_row_template(worksheet)
        for row in self._get_data_rows(worksheet):
            # TODO row_template.do_import should return a structured abstraction
            json = row_template.do_import(row)
            records[json[conversion_strategy.OBJECT_ID_FIELD]] = {
                'content': json[conversion_strategy.CONTENT_FIELD],
                'links_by_entity': json[conversion_strategy.LINKS_FIELD]
            }
        return records

    def _get_data_rows(self, worksheet):
        return worksheet.iter_rows(row_offset=self.START_ROW_IDX,
                                   max_row=(worksheet.max_row - self.START_ROW_IDX))


class ObjectListTracker(object):

    def __init__(self):
        self.ontology_values = {}

    def _get_field(self, field_chain):
        match = re.search('(?P<field_chain>.*)(\.\w+)', field_chain)
        return match.group('field_chain')

    def _get_subfield(self, field_chain):
        match = re.search('(.*)\.(?P<field>\w+)', field_chain)
        return match.group('field')

    def track_value(self, header_name, value):
        subfield = self._get_subfield(header_name)
        field = self._get_field(header_name)

        if not self.ontology_values.get(field):
            self.ontology_values[field] = {}

        self.ontology_values[field][subfield] = value

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

