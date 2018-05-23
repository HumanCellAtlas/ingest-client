import os
import unittest
from unittest import TestCase

from mock import MagicMock, patch
from openpyxl import Workbook

from ingest.importer.conversion import conversion_strategy
from ingest.importer.data_node import DataNode
from ingest.importer.importer import WorksheetImporter, WorkbookImporter, IngestImporter
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook

BASE_PATH = os.path.dirname(__file__)

HEADER_ROW = 4


def _create_single_row_worksheet(worksheet_data: dict):
    workbook = Workbook()
    worksheet = workbook.create_sheet()

    for column, data in worksheet_data.items():
        key, value = data
        worksheet[f'{column}{HEADER_ROW}'] = key
        worksheet[f'{column}6'] = value

    return worksheet


class WorkbookImporterTest(TestCase):

    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import(self, worksheet_importer_constructor):
        # given: set up template manager
        key_label_map = {
            'Project': 'project',
            'Cell Suspension': 'cell_suspension'
        }

        domain_entity_map = {
            'project': 'project',
            'cell_suspension': 'biomaterial'
        }

        mock_template_manager = MagicMock()
        mock_template_manager.get_concrete_entity_of_tab = lambda key: key_label_map.get(key)
        mock_template_manager.get_domain_entity = lambda key: domain_entity_map.get(key)

        # and: set up worksheet importer
        worksheet_importer = WorksheetImporter()
        expected_json = self._fake_worksheet_import(worksheet_importer, mock_template_manager)

        # and: set up workbook
        workbook = Workbook()
        ingest_workbook = IngestWorkbook(workbook)
        schema_list = self._mock_get_schemas(ingest_workbook)
        self._mock_importable_worksheets(ingest_workbook, workbook)

        # and: mock WorksheetImporter constructor
        worksheet_importer_constructor.return_value = worksheet_importer
        workbook_importer = WorkbookImporter(mock_template_manager)

        # when:
        workbook_output = workbook_importer.do_import(ingest_workbook)

        # then:
        self.assertEqual(2, len(list(workbook_output.keys())))
        self.assertEqual(['project', 'biomaterial'], list(workbook_output.keys()))
        self.assertEqual(2, len(list(workbook_output['project'].keys())))
        self.assertEqual(expected_json['project'], workbook_output['project'])
        self.assertEqual(expected_json['biomaterial'], workbook_output['biomaterial'])

    def _mock_get_schemas(self, ingest_workbook):
        schema_base_url = 'https://schema.humancellatlas.org'
        schema_list = [
            f'{schema_base_url}/type/project',
            f'{schema_base_url}/type/biomaterial'
        ]
        ingest_workbook.get_schemas = MagicMock(return_value=schema_list)
        return schema_list

    def _mock_importable_worksheets(self, ingest_workbook, workbook):
        project_worksheet = workbook.create_sheet('Project')
        cell_suspension_worksheet = workbook.create_sheet('Cell Suspension')
        ingest_workbook.importable_worksheets = MagicMock(return_value=[
            project_worksheet, cell_suspension_worksheet
        ])

    def _fake_worksheet_import(self, worksheet_importer: WorksheetImporter, mock_template_manager):
        projects = {
            'project 1': {'short_name': 'project 1', 'description': 'first project'},
            'project 2': {'short_name': 'project 2', 'description': 'second project'}
        }

        cell_suspensions = {
            'cell_suspension_101': {'biomaterial_id': 'cell_suspension_101', 'biomaterial_name': 'cell suspension'}
        }

        worksheet_iterator = iter([projects, cell_suspensions])
        worksheet_importer.do_import = (
            lambda __, tm: worksheet_iterator.__next__() if tm is mock_template_manager else []
        )

        return {
            'project': projects,
            'biomaterial': cell_suspensions
        }


class WorksheetImporterTest(TestCase):

    def test_do_import(self):
        # given:
        row_template = MagicMock('row_template')

        # and:
        john_doe_content = {'name': 'John Doe'}
        john_doe_links = {}
        john_doe = self._create_test_json('profile_1', john_doe_content, john_doe_links)

        # and:
        emma_jackson_content = {'name': 'Emma Jackson'}
        emma_jackson_links = {'friends': ['profile_19', 'profile_8']}
        emma_jackson = self._create_test_json('profile_2', emma_jackson_content, emma_jackson_links)

        # and:
        row_template.do_import = MagicMock('import_row', side_effect=[john_doe, emma_jackson])

        # and:
        mock_template_manager = MagicMock('template_manager')
        mock_template_manager.create_row_template = MagicMock(return_value=row_template)
        mock_template_manager.get_concrete_entity_of_tab = MagicMock(return_value='profile')

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('user_profile')
        worksheet['A6'] = 'john'
        worksheet['A7'] = 'emma'

        # when:
        worksheet_importer = WorksheetImporter()
        result = worksheet_importer.do_import(worksheet, mock_template_manager)

        # then:
        profile = result.get('profile')
        self.assertIsNotNone(profile)
        self.assertEqual(2, len(profile.keys()))

        # and:
        self._assert_correct_profile(profile, 'profile_1', john_doe_content, john_doe_links)
        self._assert_correct_profile(profile, 'profile_2', emma_jackson_content, emma_jackson_links)

    @staticmethod
    def _create_test_json(id, content, links):
        test_json = DataNode()
        test_json[f'{conversion_strategy.OBJECT_ID_FIELD}'] = id
        test_json[f'{conversion_strategy.CONTENT_FIELD}'] = content
        test_json[f'{conversion_strategy.LINKS_FIELD}'] = links
        return test_json

    def _assert_correct_profile(self, profile, profile_id, expected_content, expected_links):
        profile_1 = profile.get(profile_id)
        self.assertIsNotNone(profile_1)
        self.assertEqual(expected_content, profile_1.get('content'))
        self.assertEqual(expected_links, profile_1.get('links_by_entity'))


class IngestImporterTest(TestCase):

    @unittest.skip
    def test_import_spreadsheet(self):

        spreadsheet_file = BASE_PATH + '/metadata_spleen_new_protocols.xlsx'

        submission = IngestImporter(MagicMock()).import_spreadsheet(file_path=spreadsheet_file, submission_url=None, dry_run=True)

        self.assertTrue(submission)
