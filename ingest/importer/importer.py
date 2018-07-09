import openpyxl, logging

from ingest.importer.conversion import template_manager
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook
from ingest.importer.submission import IngestSubmitter, EntityMap, EntityLinker

format = '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=format)

class XlsImporter:

    # TODO why does the importer need to refer to an IngestApi instance?
    # Seems like it should be the IngestSubmitter that takes care of this detail
    def __init__(self, ingest_api):
        self.ingest_api = ingest_api

    def dry_run_import_file(self, file_path, project_uuid=None):
        spreadsheet_json, template_mgr = self._generate_spreadsheet_json(file_path, project_uuid)
        entity_map = self._process_links_from_spreadsheet(template_mgr, spreadsheet_json)

        return entity_map

    def _generate_spreadsheet_json(self, file_path, project_uuid=None):
        ingest_workbook = self._create_ingest_workbook(file_path)
        template_mgr = template_manager.build(ingest_workbook.get_schemas(), self.ingest_api)
        workbook_importer = WorkbookImporter(template_mgr)
        spreadsheet_json = workbook_importer.do_import(ingest_workbook, project_uuid)
        return spreadsheet_json, template_mgr

    def import_file(self, file_path, submission_url, project_uuid=None):
        spreadsheet_json, template_mgr = self._generate_spreadsheet_json(file_path, project_uuid)
        entity_map = self._process_links_from_spreadsheet(template_mgr, spreadsheet_json)

        submitter = IngestSubmitter(self.ingest_api)

        # TODO the submission_url should be passed to the IngestSubmitter instead
        submission = submitter.submit(entity_map, submission_url)
        print(f'Submission in {submission_url} is done!')  # TODO log or remove this

        return submission

    @staticmethod
    def _create_ingest_workbook(file_path):
        workbook = openpyxl.load_workbook(filename=file_path)
        return IngestWorkbook(workbook)

    @staticmethod
    def _process_links_from_spreadsheet(template_mgr, spreadsheet_json):
        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(template_mgr)
        entity_map = entity_linker.process_links_from_spreadsheet(entity_map)
        return entity_map


class WorkbookImporter:

    def __init__(self, template_mgr):
        self.worksheet_importer = WorksheetImporter()
        self.template_mgr = template_mgr

    def do_import(self, workbook: IngestWorkbook, project_uuid=None):
        spreadsheet_json = {}

        self.import_or_reference_project(project_uuid, spreadsheet_json, workbook)

        for worksheet in workbook.importable_worksheets():
            concrete_entity = self.template_mgr.get_concrete_entity_of_tab(worksheet.title)

            # TODO what if the tab is not a valid entity?
            if concrete_entity is None:
                print(f'{worksheet.title} is not a valid tab name.')
                continue

            domain_entity = self.template_mgr.get_domain_entity(concrete_entity)

            if domain_entity is None:
                continue

            entities_dict = self.worksheet_importer.do_import(worksheet, self.template_mgr)
            if spreadsheet_json.get(domain_entity) is None:
                spreadsheet_json[domain_entity] = {}

            spreadsheet_json[domain_entity].update(entities_dict)

        return spreadsheet_json

    def import_or_reference_project(self, project_uuid, spreadsheet_json, workbook):
        project_dict = None
        if not project_uuid:
            project_dict = self.import_project(workbook)
        else:
            project_dict = self.create_project_dict(project_uuid)
        spreadsheet_json['project'] = project_dict

    def import_project(self, workbook):
        project_worksheet = workbook.get_project_worksheet()
        project_importer = ProjectWorksheetImporter()
        project_dict = project_importer.do_import(project_worksheet, self.template_mgr)
        contact_worksheet = workbook.get_contact_worksheet()

        if contact_worksheet:
            contact_importer = ContactWorksheetImporter()
            contacts = contact_importer.do_import(contact_worksheet, self.template_mgr)
            project_record = list(project_dict.values())[0]
            project_record['content']['contributors'] = list(map(lambda record: record['content']['contributors'][0], contacts))

        return project_dict

    def create_project_dict(self, project_id):
        project_dict = {}
        project_dict[project_id] = {}
        project_dict[project_id]['is_reference'] = True

        return project_dict


class WorksheetImporter:

    KEY_HEADER_ROW_IDX = 4
    USER_FRIENDLY_HEADER_ROW_IDX = 2
    START_ROW_IDX = 5

    UNKNOWN_ID_PREFIX = '_unknown_'

    def __init__(self):
        self.unknown_id_ctr = 0
        self.logger = logging.getLogger(__name__)

    def do_import(self, worksheet, template: TemplateManager):
        row_template = template.create_row_template(worksheet)
        return self._import_using_row_template(template, worksheet, row_template)

    def _import_using_row_template(self, template, worksheet, row_template):
        records = {}
        header_row = template.get_header_row(worksheet)

        for index, row in enumerate(self._get_data_rows(worksheet)):
            row = row[:len(header_row)]
            if all(cell.value is None for cell in row):
                self.logger.warning(f'skipping row {index} of {worksheet.title} tab')
                continue
            metadata = row_template.do_import(row)
            record_id = self._determine_record_id(metadata)
            records[record_id] = {
                'content': metadata.content.as_dict(),
                'links_by_entity': metadata.links,
                'external_links_by_entity': metadata.external_links,
                'linking_details': metadata.linking_details
            }
        return records

    def _determine_record_id(self, metadata):
        record_id = metadata.object_id
        if record_id is None:
            record_id = self._generate_id()
        return record_id

    def _generate_id(self):
        self.unknown_id_ctr = self.unknown_id_ctr + 1
        return f'{self.UNKNOWN_ID_PREFIX}{self.unknown_id_ctr}'

    def _get_data_rows(self, worksheet):
        return worksheet.iter_rows(row_offset=self.START_ROW_IDX,
                                  max_row=(worksheet.max_row - self.START_ROW_IDX))


class ProjectWorksheetImporter(WorksheetImporter):

    def do_import(self, worksheet, template: TemplateManager):
        records = super(ProjectWorksheetImporter, self).do_import(worksheet, template)

        if len(records.keys()) == 0:
            raise NoProjectFound()

        if len(records.keys()) > 1:
            raise MultipleProjectsFound()

        return records


class ContactWorksheetImporter(WorksheetImporter):

    def do_import(self, worksheet, template: TemplateManager):
        row_template = template.create_simple_row_template(worksheet)
        records = self._import_using_row_template(template, worksheet, row_template)

        return list(records.values())


class MultipleProjectsFound(Exception):
    pass


class NoProjectFound(Exception):
    pass
