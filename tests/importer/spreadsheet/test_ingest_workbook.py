from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook, IngestWorksheet
from tests.importer.utils.test_utils import create_test_workbook


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
        importable_names = ['Organ From Donor', 'Cell Suspension', 'Project']
        workbook = create_test_workbook(*importable_names)
        workbook.create_sheet('Schemas')

        # and:
        ingest_workbook = IngestWorkbook(workbook)

        # when:
        actual_worksheets = ingest_workbook.importable_worksheets()

        # then:
        actual_titles = [ingest_worksheet.title for ingest_worksheet in actual_worksheets]
        self.assertEqual(importable_names, actual_titles)


class IngestWorksheetTest(TestCase):

    def test_get_title(self):
        # given:
        workbook = create_test_workbook('User', 'User - SN Profiles')
        user_sheet = workbook.get_sheet_by_name('User')
        sn_profiles_sheet = workbook.get_sheet_by_name('User - SN Profiles')

        # and:
        user = IngestWorksheet(user_sheet)
        sn_profiles = IngestWorksheet(sn_profiles_sheet)

        # expect:
        self.assertEqual('User', user.title)
        self.assertEqual('User - SN Profiles', sn_profiles.title)

    def test_set_title(self):
        # given:
        workbook = create_test_workbook('User')
        user_sheet = workbook.get_sheet_by_name('User')
        user = IngestWorksheet(user_sheet)

        # and: assume
        self.assertEqual('User', user.title)

        # when:
        user.title = 'Account'

        # then:
        self.assertEqual('Account', user.title)

    def test_is_module_tab(self):
        # given:
        workbook = create_test_workbook('Product', 'Product - History')
        product_sheet = workbook.get_sheet_by_name('Product')
        history_sheet = workbook.get_sheet_by_name('Product - History')

        # and:
        product = IngestWorksheet(product_sheet)
        history = IngestWorksheet(history_sheet)

        # expect:
        self.assertFalse(product.is_module_tab())
        self.assertTrue(history.is_module_tab())