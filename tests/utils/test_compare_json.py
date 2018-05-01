from unittest import TestCase

import json

from ingest.utils.compare_json import compare_json_data


def load_json(file_path):
    # open JSON file and parse contents
    fh = open(file_path, 'r')
    data = json.load(fh)
    fh.close()

    return data


class TestCompareJson(TestCase):

    def test_compare_json_data(self):
        a_json = load_json('a.json')
        b_json = load_json('b.json')

        self.assertTrue(compare_json_data(a_json, b_json))
