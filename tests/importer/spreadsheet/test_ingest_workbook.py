from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook


class IngestWorkbookTest(TestCase):

    def test_get_schemas(self):
        # given:
        workbook = Workbook()

        # and:
        schemas_sheet = workbook.create_sheet('Schemas')
        schemas_sheet['A1'] = 'schema'

        # and:
        base_url = 'https://schema.humancellatlas.org'
        expected_schemas = [
            f'{base_url}/type/biomaterial/cell_suspension',
            f'{base_url}type/biomaterial/organ_from_donor',
            f'{base_url}/type/process/library_preparation'
        ]
        header_offset = 2
        for index, schema in enumerate(expected_schemas):
            schemas_sheet[f'A{index + header_offset}'] = schema

        # and:
        ingest_workbook = IngestWorkbook(workbook)

        # when:
        actual_schemas = ingest_workbook.get_schemas()

        # then:
        self.assertEqual(expected_schemas, actual_schemas)

    def test_importable_worksheets(self):
        # given:
        workbook = Workbook()

        # and:
        importable_names = ['Organ From Donor', 'Cell Suspension']
        expected_worksheets = [workbook.create_sheet(name) for name in importable_names]
        workbook.create_sheet('Schemas')

        # and:
        default_worksheet = workbook.get_sheet_by_name('Sheet')
        workbook.remove_sheet(default_worksheet)

        # and:
        ingest_workbook = IngestWorkbook(workbook)

        # when:
        actual_worksheets = ingest_workbook.importable_worksheets()

        # then:
        self.assertEqual(expected_worksheets, actual_worksheets)
