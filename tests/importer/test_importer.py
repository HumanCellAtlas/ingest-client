import os
import unittest

from unittest import TestCase

from mock import MagicMock, patch
from openpyxl import Workbook

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion import template_manager
from ingest.importer.conversion.data_converter import (
    Converter, ListConverter, BooleanConverter, DataType
)
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
        john_doe = DataNode()
        john_doe[f'{conversion_strategy.OBJECT_ID_FIELD}'] = 'profile_1'
        john_doe[f'{conversion_strategy.CONTENT_FIELD}'] = {'name': 'John Doe'}
        john_doe[f'{conversion_strategy.LINKS_FIELD}'] = {}
        row_template.do_import = MagicMock('import_row', return_value=john_doe)

        # and:
        template_manager = MagicMock('template_manager')
        template_manager.create_row_template = MagicMock(return_value=row_template)

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('user_profile')

        # when:
        worksheet_importer = WorksheetImporter()
        result = worksheet_importer.do_import(worksheet, template_manager)

        # then:
        self.assertIsNotNone(result)

    def test_do_import_with_object_list_fields(self):
        # given:
        template_mgr = MagicMock(name='template_manager')
        template_mgr.create_template_node = lambda __: DataNode()
        template_mgr.get_converter = MagicMock(return_value=Converter())
        template_mgr.get_schema_url = MagicMock(return_value='url')
        template_mgr.get_schema_type = MagicMock(return_value='type')
        template_mgr.is_identifier_field = MagicMock(side_effect=(
            lambda header_name: True if header_name == 'project.id_column' else False
        ))
        concrete_entity_map = {
            'project.id_column': 'project'
        }
        template_mgr.get_concrete_entity_of_column = lambda key: concrete_entity_map.get(key)
        template_mgr.get_concrete_entity_of_tab = lambda key: 'project'
        template_mgr.get_key_for_label = MagicMock(side_effect=lambda key, tab: key)

        # and:
        multivalue_fields = {
            'project.genus_species.ontology': True,
            'project.genus_species.text': True,
        }

        template_mgr.is_parent_field_multivalue = (
            lambda field_name: multivalue_fields.get(field_name)
        )

        # and:
        worksheet = _create_single_row_worksheet({
            'A': ('project.genus_species.ontology', 'UO:000008'),
            'B': ('project.genus_species.text', 'meter'),
            'C': ('project.id_column', 'id'),
        })

        # and:
        worksheet_importer = WorksheetImporter()

        # when:
        rows_by_id = worksheet_importer.do_import(worksheet, template_mgr)

        # then:
        self.assertEqual(1, len(rows_by_id))
        json = rows_by_id['id']['content']

        # and:
        self.assertTrue(type(json['genus_species']) is list)
        self.assertEqual(1, len(json['genus_species']))
        self.assertEqual({'ontology': 'UO:000008', 'text': 'meter'}, json['genus_species'][0])

    @patch('ingest.importer.importer.ObjectListTracker')
    def test_do_import_builds_from_template(self, object_list_tracker_constructor):
        # given:

        mock_template_manager = MagicMock(name='template_manager')
        mock_template_manager.get_converter = MagicMock(return_value=Converter())
        mock_template_manager.is_parent_field_multivalue = MagicMock(return_value=False)
        mock_template_manager.is_identifier_field = (
            lambda header_name: True if header_name == 'project.short_name' else False
        )
        concrete_entity_map = {
            'project.short_name': 'project'
        }
        mock_template_manager.get_concrete_entity_of_column = lambda key: concrete_entity_map.get(key)
        mock_template_manager.get_concrete_entity_of_tab = lambda key: 'project'
        mock_template_manager.get_key_for_label = MagicMock(side_effect=lambda key, tab: key)

        # and:
        node_template = DataNode()
        node_template['describedBy'] = 'https://schemas.sample.com/test'
        node_template['extra_field'] = 'an extra field'
        node_template['version'] = '0.0.1'
        mock_template_manager.create_template_node = lambda __: node_template

        # and:
        object_list_tracker_constructor.return_value = MagicMock(name='object_list_tracker')

        # and:
        importer = WorksheetImporter()

        # and:
        worksheet = _create_single_row_worksheet({
            'A': ('project.short_name', 'Project'),
            'B': ('project.description', 'This is a project')
        })

        # when:
        rows_by_id = importer.do_import(worksheet, mock_template_manager)

        # then:
        self.assertEqual(1, len(list(rows_by_id.keys())))
        json = rows_by_id['Project']['content']

        # and:
        self.assertEqual('https://schemas.sample.com/test', json.get('describedBy'))
        self.assertEqual('an extra field', json.get('extra_field'))
        self.assertEqual('0.0.1', json.get('version'))


class IngestImporterTest(TestCase):

    @unittest.skip
    def test_import_spreadsheet(self):
        spreadsheet_file = BASE_PATH + '/metadata_spleen_new_protocols.xlsx'

        submission = IngestImporter.import_spreadsheet(spreadsheet_file, 'token')

        self.assertTrue(submission)
