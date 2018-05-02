import json
import os
from os import listdir
from os.path import isfile, join
from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.hcaxlsbroker import SpreadsheetSubmission
from ingest.importer.importer import TabImporter, MetadataMapping
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


class TabImporterTest(TestCase):

    def test_import(self):
        # given:
        column_mapping = {
            'Short name': 'project_shortname',
            'Project title': 'project_title'
        }

        # and:
        mapping = MetadataMapping()
        mapping.get_column_mapping = lambda display_name: column_mapping[display_name]
        tab_importer = TabImporter(mapping, [
                'projects.project.project_core.project_shortname',
                'projects.project.project_core.project_title'
            ])

        # and:
        workbook = Workbook()
        tab = workbook.create_sheet('Project')
        tab['A1'] = 'Short name'
        tab['A4'] = 'Tissue stability'
        tab['B1'] = 'Project title'
        tab['B4'] = 'Ischaemic sensitivity of human tissue by single cell RNA seq.'

        # when:
        json = tab_importer.do_import(tab)

        # then:
        self.assertTrue(json)
        project_core = json['project_core']
        self.assertEqual('Tissue stability', project_core['project_shortname'])
        self.assertEqual('Ischaemic sensitivity of human tissue by single cell RNA seq.',
                         project_core['project_title'])
