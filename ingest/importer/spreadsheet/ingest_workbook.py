from openpyxl import Workbook

from ingest.importer.spreadsheet.ingest_worksheet import IngestWorksheet

SCHEMAS_WORKSHEET = 'Schemas'
SPECIAL_TABS = [SCHEMAS_WORKSHEET]


class IngestWorkbook:

    def __init__(self, workbook: Workbook):
        self.workbook = workbook

    def get_worksheet(self, worksheet_title):
        if worksheet_title in self.workbook.get_sheet_names():
            return self.workbook[worksheet_title]

        if worksheet_title.lower() in self.workbook.get_sheet_names():
            return self.workbook[worksheet_title.lower()]

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

        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            schema_cell = row[0]
            schemas.append(schema_cell.value)
        return schemas

    def importable_worksheets(self):
        return [IngestWorksheet(worksheet) for worksheet in self.workbook.worksheets
                if worksheet.title not in SPECIAL_TABS]
