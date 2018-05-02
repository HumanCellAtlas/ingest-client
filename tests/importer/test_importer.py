import json
import os

from unittest import TestCase
from os import listdir
from os.path import isfile, join

import yaml
from openpyxl import Workbook

from ingest.importer.importer import Importer
from ingest.utils.compare_json import compare_json_data

from ingest.importer.hcaxlsbroker import SpreadsheetSubmission

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


class ImporterTest(TestCase):

    def test_import(self):
        # given:
        with open('data/spleen_config.yaml', 'r') as spec_file:
            yaml_spec = yaml.load(spec_file)
        importer = Importer(yaml_spec)

        # and:
        workbook = Workbook()
        project_sheet = workbook.create_sheet('Project')
        project_sheet['A1'] = 'Short name'
        project_sheet['A4'] = 'Tissue stability'
        project_sheet['B1'] = 'Project title'
        project_sheet['B4'] = 'Ischaemic sensitivity of human tissue by single cell RNA seq.'

        # when:
        json = importer.do_import(project_sheet)

        # then:
        self.assertTrue(json)
        project_core = json['project_core']
        self.assertEqual('Tissue stability', project_core['project_shortname'])
        self.assertEqual('Ischaemic sensitivity of human tissue by single cell RNA seq.',
                         project_core['project_title'])
