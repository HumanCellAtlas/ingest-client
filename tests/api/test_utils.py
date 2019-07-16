from datetime import datetime
from unittest import TestCase

from ingest.api import utils


class UtilsTest(TestCase):

    def test_parse_date_string(self):
        # given:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)

        # and:
        iso_date = '2019-06-12T09:49:25.000Z'
        iso_date_short = '2019-06-12T09:49:25Z'
        dss_version_date = '2019-06-12T094925.000000Z'
        weird_date = '2019:06:12Y09-49-25.000X'

        # expect:
        self.assertEqual(expected_date, utils.parse_date_string(iso_date))
        self.assertEqual(expected_date, utils.parse_date_string(iso_date_short))
        self.assertEqual(expected_date, utils.parse_date_string(dss_version_date))

        # and:
        with self.assertRaises(ValueError):
            utils.parse_date_string(weird_date)

    def test_to_dss_version(self):
        # given:
        date_string = '2019-05-23T16:53:40.931Z'

        # expect:
        self.assertEqual('2019-05-23T165340.931000Z', utils.to_dss_version(date_string))

    def test_to_dss_version_with_already_formatted_date(self):
        # given:
        date_string = '2019-05-23T165340.931000Z'

        # expect:
        self.assertEqual(date_string, utils.to_dss_version(date_string))
