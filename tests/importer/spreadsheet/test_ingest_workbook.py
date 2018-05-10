from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.importer import IngestWorkbook


class IngestWorkbookTest(TestCase):

    def test_get_schemas(self):
        # given:
        workbook = Workbook()

        # and:
        schemas_sheet = workbook.create_sheet('schemas')
        schemas_sheet['A1'] = 'schema'

        # and:
        base_url = 'https://schema.humancellatlas.org'
        expected_schemas = [
            f'{base_url}/type/biomaterial/cell_suspension',
            f'{base_url}type/biomaterial/organ_from_donor',
            f'{base_url}/type/process/library_preparation'
        ]
        for index, schema in enumerate(expected_schemas):
            schemas_sheet[f'A{index + 1}'] = schema

        # and:
        ingest_workbook = IngestWorkbook(workbook)

        # when:
        actual_schemas = ingest_workbook.get_schemas()

        # then:
        self.assertEqual(len(expected_schemas), len(actual_schemas))
