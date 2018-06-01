from openpyxl import Workbook

SCHEMAS_WORKSHEET = 'Schemas'
PROJECT_WORKSHEET = 'Project'
CONTACT_WORKSHEET = 'Contact'

# TODO think of a better name
SPECIAL_TABS = [SCHEMAS_WORKSHEET, PROJECT_WORKSHEET, CONTACT_WORKSHEET]


class IngestWorkbook:

    def __init__(self, workbook: Workbook):
        self.workbook = workbook

    def get_project_worksheet(self):
        if PROJECT_WORKSHEET in self.workbook.get_sheet_names():
            return self.workbook[PROJECT_WORKSHEET]

        if PROJECT_WORKSHEET.lower() in self.workbook.get_sheet_names():
            return self.workbook[PROJECT_WORKSHEET.lower()]

        return None

    def get_contact_worksheet(self):
        if CONTACT_WORKSHEET in self.workbook.get_sheet_names():
            return self.workbook[CONTACT_WORKSHEET]

        if CONTACT_WORKSHEET.lower() in self.workbook.get_sheet_names():
            return self.workbook[CONTACT_WORKSHEET.lower()]

        return None

    def get_schemas(self):
        schemas = []

        worksheet = None

        if SCHEMAS_WORKSHEET in self.workbook.get_sheet_names():
            worksheet = self.workbook.get_sheet_by_name(SCHEMAS_WORKSHEET)

        if SCHEMAS_WORKSHEET.lower() in self.workbook.get_sheet_names():
            worksheet = self.workbook.get_sheet_by_name(SCHEMAS_WORKSHEET.lower())

        if not worksheet:
            return schemas

        for row in worksheet.iter_rows(row_offset=1, max_row=(worksheet.max_row - 1)):
            schema_cell = row[0]
            schemas.append(schema_cell.value)
        return schemas

    def importable_worksheets(self):
        importable_names = [name for name in self.workbook.get_sheet_names() if
                            (not name in SPECIAL_TABS)]
        return [self.workbook[name] for name in importable_names]
