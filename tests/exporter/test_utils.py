from unittest import TestCase

from ingest.exporter import utils


class UtilsTest(TestCase):

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
