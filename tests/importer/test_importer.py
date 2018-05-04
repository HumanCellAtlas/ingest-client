import json
import os
from os import listdir
from os.path import isfile, join
from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.hcaxlsbroker import SpreadsheetSubmission
from ingest.importer.importer import WorksheetImporter
from ingest.importer.schematemplate import SchemaTemplate
from ingest.utils.compare_json import compare_json_data

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


class TestImporter(TestCase):

    def test_submit_dryrun(self):
        actual_output_dir = BASE_PATH + '/output/actual/'
        expected_output_dir = BASE_PATH + '/output/expected/'

        empty_dir(actual_output_dir)

        spreadsheet_path = './glioblastoma_v5_plainHeaders_small_2cells.xlsx'
        submission = SpreadsheetSubmission(dry=True, output=actual_output_dir)
        submission.submit2(spreadsheet_path, None, None, 'glioblastoma')

        self._compare_files_in_dir(expected_output_dir, actual_output_dir)

    def _compare_files_in_dir(self, dir1, dir2):
        expected_files = [f for f in listdir(dir1) if isfile(join(dir1, f))]

        for filename in expected_files:
            a_json = load_json(join(dir1, filename))
            b_json = load_json(join(dir2, filename))

            self.assertTrue(compare_json_data(a_json, b_json), 'discrepancy in ' + filename)


class WorksheetImporterTest(TestCase):

    def test_do_import(self):
        # given:
        worksheet_importer = WorksheetImporter()

        # and:
        cell_mapping = {
            'projects.project.project_core.project_shortname': {
                'value_type': 'string',
                'multivalue': False
            },
            'projects.project.miscellaneous': {
                'value_type': 'string',
                'multivalue': True
            },
            'projects.project.numbers': {
                'value_type': 'integer',
                'multivalue': True
            }
        }

        # and:
        schema_template = SchemaTemplate()
        schema_template.lookup = lambda key: cell_mapping.get(key)

        # and:
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

        # when:
        json = worksheet_importer.do_import(worksheet, schema_template)

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
