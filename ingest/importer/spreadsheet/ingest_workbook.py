from openpyxl import Workbook


class IngestWorkbook:

    def __init__(self, workbook: Workbook):
        self.workbook = workbook

    def get_schemas(self):
        worksheet = self.workbook.get_sheet_by_name('schemas')
        schemas = []
        for row in worksheet.iter_rows(row_offset=1, max_row=(worksheet.max_row - 1)):
            schema_cell = row[0]
            schemas.append(schema_cell.value)
        return schemas
