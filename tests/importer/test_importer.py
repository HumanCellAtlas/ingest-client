import json
import os
from os import listdir
from os.path import isfile, join
from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.conversion.data_converter import Converter, ListConverter, BooleanConverter, \
    DataType
from ingest.importer.importer import WorksheetImporter, TemplateManager

BASE_PATH = os.path.dirname(__file__)


def load_json(file_path):
    # open JSON file and parse contents
    fh = open(file_path, 'r')
    data = json.load(fh)
    fh.close()

    return data


def empty_dir(directory):
    files = [f for f in listdir(directory) if isfile(join(directory, f))]

    for filename in files:
        os.unlink(join(directory, filename))


class WorksheetImporterTest(TestCase):

    def test_do_import(self):
        # given:
        worksheet_importer = WorksheetImporter()

        # and:
        converter_mapping = {
            'projects.project.project_core.project_shortname': Converter(),
            'projects.project.miscellaneous': ListConverter(),
            'projects.project.numbers': ListConverter(data_type=DataType.INTEGER),
            'projects.project.is_active': BooleanConverter(),
            'projects.project.is_submitted': BooleanConverter()
        }

        # and:
        template_manager = TemplateManager()
        template_manager.get_converter = (
            lambda key: converter_mapping.get(key) if key in converter_mapping else Converter()
        )

        # and:
        worksheet = self._create_test_worksheet()

        # when:
        json = worksheet_importer.do_import(worksheet, template_manager)

        # then:
        self.assertTrue(json)
        project_core = json['project_core']
        self.assertEqual('Tissue stability', project_core['project_shortname'])
        self.assertEqual('Ischaemic sensitivity of human tissue by single cell RNA seq.',
                         project_core['project_title'])

        # and:
        self.assertEqual(2, len(json['miscellaneous']))
        self.assertEqual(['extra', 'details'], json['miscellaneous'])

        # and:
        self.assertEqual(7, json['contributor_count'])

        # and
        self.assertEqual('Juan Dela Cruz||John Doe', json['contributors'])

        # and
        self.assertEqual([1, 2, 3], json['numbers'])

        # and
        self.assertEqual(True, json['is_active'])
        self.assertEqual(False, json['is_submitted'])



    def _create_test_worksheet(self):
        workbook = Workbook()
        worksheet = workbook.create_sheet('Project')
        worksheet['A1'] = 'projects.project.project_core.project_shortname'
        worksheet['A4'] = 'Tissue stability'
        worksheet['B1'] = 'projects.project.project_core.project_title'
        worksheet['B4'] = 'Ischaemic sensitivity of human tissue by single cell RNA seq.'
        worksheet['C1'] = 'projects.project.miscellaneous'
        worksheet['C4'] = 'extra||details'
        worksheet['D1'] = 'projects.project.contributor_count'
        worksheet['D4'] = 7
        worksheet['E1'] = 'projects.project.contributors'
        worksheet['E4'] = 'Juan Dela Cruz||John Doe'
        worksheet['F1'] = 'projects.project.numbers'
        worksheet['F4'] = '1||2||3'
        worksheet['G1'] = 'projects.project.is_active'
        worksheet['G4'] = 'Yes'
        worksheet['H1'] = 'projects.project.is_submitted'
        worksheet['H4'] = 'No'

        return worksheet
