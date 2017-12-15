from unittest import TestCase

from mock import patch

from broker import broker_app
from broker.broker_app import app
from broker.hcaxlsbroker import SpreadsheetSubmission


class BrokerAppTest(TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_authorization_failed(self):
        # when:
        response = self.client.post('/upload')

        # then:
        self.assertEqual(401, response.status_code)

    def test_failed_save(self):
        with patch.object(broker_app, '_save_file') as save_file:
            # given:
            assert save_file is broker_app._save_file
            save_file.side_effect = Exception("I/O error")

            # when:
            response = self.client.post('/upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(500, response.status_code)

    def test_value_error_on_spreadsheet_submission(self):
        with patch.object(broker_app, '_save_file') as save_file, \
                patch.object(SpreadsheetSubmission, 'submit') as submit_spreadsheet:
            # given:
            save_file.return_value = 'path/to/file.xls'
            submit_spreadsheet.side_effect = ValueError("value error")

            # when:
            response = self.client.post('/upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(400, response.status_code)

    def test_key_error_on_spreadsheet_submission(self):
        with patch.object(broker_app, '_save_file') as save_file, \
                patch.object(SpreadsheetSubmission, 'submit') as submit_spreadsheet:
            # given:
            save_file.return_value = 'path/to/file.xls'
            submit_spreadsheet.side_effect = KeyError("key error")

            # when:
            response = self.client.post('/upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(400, response.status_code)