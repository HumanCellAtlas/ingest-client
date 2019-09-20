import logging

from ingest.importer.conversion import template_manager
from ingest.importer.conversion.metadata_entity import MetadataEntity
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook
from ingest.importer.spreadsheet.ingest_worksheet import IngestWorksheet
from ingest.importer.submission import IngestSubmitter, EntityMap, EntityLinker, Submission
from ingest.template.exceptions import UnknownKeySchemaException
from ingest.utils.IngestError import ImporterError, ParserError

format = '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=format)


class XlsImporter:
    """
    XlsImporter is used to convert a contributor's spreadsheet into metadata json entities and to submit those to
    Ingest. Please see https://github.com/HumanCellAtlas/ingest-central/wiki/Data-Contributors-Spreadsheet-Quick-Guide
    for more information on the spreadsheet format.
    """

    def __init__(self, ingest_api):
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)

    def generate_json(self, file_path, project_uuid=None):
        ingest_workbook = IngestWorkbook.from_file(file_path)

        try:
            template_mgr = template_manager.build(ingest_workbook.get_schemas(), self.ingest_api)
        except Exception as e:
            raise SchemaRetrievalError(
                f'There was an error retrieving the schema information to process the spreadsheet. {str(e)}')

        workbook_importer = WorkbookImporter(template_mgr)
        spreadsheet_json, errors = workbook_importer.do_import(ingest_workbook, project_uuid)

        return spreadsheet_json, template_mgr, errors

    def import_file(self, file_path, submission_url, project_uuid=None):
        submission = None
        try:
            spreadsheet_json, template_mgr, errors = self.generate_json(file_path, project_uuid)
            if not errors:
                entity_map = self._process_links_from_spreadsheet(template_mgr, spreadsheet_json)

                # TODO the submission_url should be passed to the IngestSubmitter instead
                submitter = IngestSubmitter(self.ingest_api)
                submission = submitter.submit(entity_map, submission_url)
                self.logger.info(f'Submission in {submission_url} is done!')
            else:
                self.report_errors(submission_url, errors)
        except Exception as e:
            self.ingest_api.create_submission_error(submission_url, ImporterError(str(e)).getJSON())
            self.logger.error(str(e), exc_info=True)
        else:
            return submission, template_mgr

    def report_errors(self, submission_url, errors):
        self.logger.info(f'Logged {len(errors)} ParsingErrors.', exc_info=False)
        for error in errors:
            self.ingest_api.create_submission_error(
                submission_url,
                ParserError(error["location"], error["type"], error["detail"]).getJSON()
            )

    @staticmethod
    def _process_links_from_spreadsheet(template_mgr, spreadsheet_json):
        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(template_mgr)
        entity_map = entity_linker.process_links_from_spreadsheet(entity_map)
        return entity_map

    @staticmethod
    def create_update_spreadsheet(submission: Submission, template_mgr: TemplateManager, file_path):
        if not submission:
            return
        wb = IngestWorkbook.from_file(file_path, read_only=False)
        wb.add_entity_uuids(submission)
        wb.add_schemas_worksheet(template_mgr.get_schemas())
        return wb.save(file_path)


_PROJECT_ID = 'project_0'
_PROJECT_TYPE = 'project'


class _ImportRegistry:
    """
    This is a helper class for managing metadata entities during Workbook import.
    """

    def __init__(self, template_mgr):
        self.template_mgr = template_mgr
        self._submittable_registry = {}
        self._module_registry = {}
        self._module_list = []
        self.project_id = _PROJECT_ID

    def add_submittable(self, metadata: MetadataEntity):
        # TODO no test to check case sensitivity
        domain_type = metadata.domain_type.lower()
        type_map = self._submittable_registry.get(domain_type)
        if not type_map:
            type_map = {}
            self._submittable_registry[domain_type] = type_map
        if domain_type.lower() == _PROJECT_TYPE:
            if not type_map.get(self.project_id):
                metadata.object_id = metadata.object_id or self.project_id
                self.project_id = metadata.object_id
            else:
                raise MultipleProjectsFound()
        type_map[metadata.object_id] = metadata

    def add_submittables(self, metadata_entities):
        for entity in metadata_entities:
            self.add_submittable(entity)

    def add_module(self, metadata: MetadataEntity):
        if metadata.domain_type.lower() == 'project':
            metadata.object_id = self.project_id
        self._module_list.append(metadata)

    def add_modules(self, module_field_name, metadata_entities):
        allowed_fields = [module_field_name]
        allowed_fields.extend(self.template_mgr.default_keys)
        removed_fields = []
        for entity in metadata_entities:
            removed_fields.extend(entity.list_fields(excluded_fields=allowed_fields))
            entity.retain_fields(module_field_name)
            self.add_module(entity)
        return removed_fields

    def import_modules(self):
        for module_entity in self._module_list:
            type_map = self._submittable_registry.get(module_entity.domain_type)
            submittable_entity = type_map.get(module_entity.object_id)
            submittable_entity.add_module_entity(module_entity)

    def flatten(self):
        flat_map = {}
        for domain_type, type_map in self._submittable_registry.items():
            flat_type_map = {object_id: metadata.map_for_submission()
                             for object_id, metadata in type_map.items()}
            flat_map[domain_type] = flat_type_map
        return flat_map

    def has_project(self):
        project_registry = self._submittable_registry.get(_PROJECT_TYPE)
        return project_registry and project_registry.get(self.project_id)


class WorkbookImporter:
    def __init__(self, template_mgr):
        self.worksheet_importer = WorksheetImporter(template_mgr)
        self.template_mgr = template_mgr
        self.logger = logging.getLogger(__name__)

    def do_import(self, workbook: IngestWorkbook, project_uuid=None):
        registry = _ImportRegistry(self.template_mgr)
        importable_worksheets = workbook.importable_worksheets()

        if project_uuid:
            project_metadata = MetadataEntity(domain_type=_PROJECT_TYPE,
                                              concrete_type=_PROJECT_TYPE,
                                              object_id=project_uuid,
                                              is_reference=True,
                                              content={})
            registry.add_submittable(project_metadata)

            importable_worksheets = [ws for ws in importable_worksheets
                                     if _PROJECT_TYPE not in ws.title.lower()]
        workbook_errors = []
        for worksheet in importable_worksheets:
            try:
                self.sheet_in_schemas(worksheet)
                metadata_entities, worksheet_errors = self.worksheet_importer.do_import(worksheet)
                module_field_name = worksheet.get_module_field_name()
                workbook_errors.extend(worksheet_errors)

                if worksheet.is_module_tab():
                    removed_data = registry.add_modules(module_field_name, metadata_entities)
                    workbook_errors.extend(self.list_data_removal_errors(worksheet.title, removed_data))
                else:
                    registry.add_submittables(metadata_entities)
            except Exception as e:
                workbook_errors.append(
                    {"location": f'sheet={worksheet.title}', "type": e.__class__.__name__, "detail": str(e)})

        if registry.has_project():
            registry.import_modules()
        else:
            e = NoProjectFound()
            workbook_errors.append({"location": "File", "type": e.__class__.__name__, "detail": str(e)})
        return registry.flatten(), workbook_errors

    def sheet_in_schemas(self, worksheet):
        schemas = self.template_mgr.template.json_schemas
        try:
            concrete_type = self.template_mgr.get_concrete_type(worksheet.title)
        except UnknownKeySchemaException as e:
            raise SheetNotFoundInSchemas(worksheet.title)
        module_field_name = worksheet.get_module_field_name()
        for schema in schemas:
            if 'name' in schema or 'title' in schema:
                schema_name = schema['name'] if 'name' in schema else schema['title']
                if schema_name == concrete_type:
                    if not worksheet.is_module_tab() or module_field_name in schema['properties']:
                        return True
                    raise SheetNotFoundInSchemas(worksheet.title)
        raise SheetNotFoundInSchemas(worksheet.title)

    @staticmethod
    def list_data_removal_errors(sheet, removed_data):
        errors = []
        for data in removed_data:
            e = DataRemoval(data['key'], data['value'])
            errors.append({"location": f'sheet={sheet}', "type": e.__class__.__name__, "detail": str(e)})
        return errors


class WorksheetImporter:
    KEY_HEADER_ROW_IDX = 4
    USER_FRIENDLY_HEADER_ROW_IDX = 2
    START_ROW_OFFSET = 5

    UNKNOWN_ID_PREFIX = '_unknown_'

    def __init__(self, template: TemplateManager):
        self.template = template
        self.unknown_id_ctr = 0
        self.logger = logging.getLogger(__name__)
        self.concrete_entity = None

    def do_import(self, ingest_worksheet: IngestWorksheet):
        records = []
        worksheet_errors = []
        try:
            row_template = self.template.create_row_template(ingest_worksheet)
            rows = ingest_worksheet.get_data_rows()
            for index, row in enumerate(rows):
                metadata, row_errors = row_template.do_import(row)
                for error in row_errors:
                    if 'location' in error:
                        error["location"] = f'sheet={ingest_worksheet.title} row={index}, {error["location"]}'
                    else:
                        error["location"] = f'sheet={ingest_worksheet.title} row={index}'
                    worksheet_errors.append(error)
                if not metadata.object_id:
                    metadata.object_id = self._generate_id()
                records.append(metadata)
        except Exception as e:
            worksheet_errors.append({
                "location": f'sheet={ingest_worksheet.title}',
                "type": e.__class__.__name__,
                "detail": str(e)
            })
        return records, worksheet_errors

    def _generate_id(self):
        self.unknown_id_ctr = self.unknown_id_ctr + 1
        return f'{self.UNKNOWN_ID_PREFIX}{self.unknown_id_ctr}'


class MultipleProjectsFound(Exception):
    def __init__(self):
        message = f'The spreadsheet should only be associated to a single project.'
        super(MultipleProjectsFound, self).__init__(message)


class NoProjectFound(Exception):
    def __init__(self):
        message = f'The spreadsheet should be associated to a project.'
        super(NoProjectFound, self).__init__(message)


class SheetNotFoundInSchemas(Exception):
    def __init__(self, sheet):
        message = f'The sheet named {sheet} was not found in the schema list.'
        super(SheetNotFoundInSchemas, self).__init__(message)
        self.sheet = sheet


class DataRemoval(Exception):
    def __init__(self, key, value):
        message = f'The column header [{key}] was not recognised, the following data has been removed: {value}.'
        super(DataRemoval, self).__init__(message)
        self.key = key
        self.value = value


class SchemaRetrievalError(Exception):
    pass
