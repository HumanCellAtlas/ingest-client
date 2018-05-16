from openpyxl import Workbook

SCHEMAS_WORKSHEET = 'schemas'
PROJECT_WORKSHEET = 'project'
CONTACT_WORKSHEET = 'contact'

# TODO think of a better name
SPECIAL_TABS = [SCHEMAS_WORKSHEET, PROJECT_WORKSHEET, CONTACT_WORKSHEET]

class IngestWorkbook:

    def __init__(self, workbook: Workbook):
        self.workbook = workbook

    def get_schemas(self):
        worksheet = self.workbook.get_sheet_by_name(SCHEMAS_WORKSHEET)
        schemas = []
        for row in worksheet.iter_rows(row_offset=1, max_row=(worksheet.max_row - 1)):
            schema_cell = row[0]
            schemas.append(schema_cell.value)
        return schemas

    def importable_worksheets(self):
        importable_names = [name for name in self.workbook.get_sheet_names() if
                            (not name in SPECIAL_TABS)]
        return [self.workbook.get_sheet_by_name(name) for name in importable_names]
