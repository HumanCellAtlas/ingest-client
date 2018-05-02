import json
import os

from unittest import TestCase
from os import listdir
from os.path import isfile, join


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
