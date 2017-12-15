from unittest import TestCase

from flask import json
from mock import patch

from broker import broker_app
from broker.broker_app import app
from broker.hcaxlsbroker import SpreadsheetSubmission
from broker.ingestapi import IngestApi


class BrokerAppTest(TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_authorization_failed(self):
        # when:
        response = self.client.post('/api_upload')

        # then:
        self.assertEqual(401, response.status_code)

    def test_failed_save(self):
        with patch.object(broker_app, '_save_file') as save_file:
            # given:
            assert save_file is broker_app._save_file
            save_file.side_effect = Exception("I/O error")

            # when:
            response = self.client.post('/api_upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(500, response.status_code)

    def test_value_error_on_spreadsheet_submission(self):
        with patch.object(broker_app, '_save_file') as save_file, \
                patch.object(SpreadsheetSubmission, 'submit') as submit_spreadsheet:
            # given:
            save_file.return_value = 'path/to/file.xls'
            submit_spreadsheet.side_effect = ValueError("value error")

            # when:
            response = self.client.post('/api_upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(400, response.status_code)

    def test_key_error_on_spreadsheet_submission(self):
        with patch.object(broker_app, '_save_file') as save_file, \
                patch.object(SpreadsheetSubmission, 'submit') as submit_spreadsheet:
            # given:
            save_file.return_value = 'path/to/file.xls'
            submit_spreadsheet.side_effect = KeyError("key error")

            # when:
            response = self.client.post('/api_upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(400, response.status_code)

    def test_submission_successful(self):
        with patch.object(broker_app, '_save_file') as save_file, \
                patch.object(SpreadsheetSubmission, 'submit') as submit_spreadsheet, \
                patch.object(SpreadsheetSubmission, 'createSubmission') as create_submission, \
                patch.object(IngestApi, '__init__') as init_ingest_api, \
                patch.object(IngestApi, 'getObjectUuid') as get_object_uuid:
            # given:
            save_file.return_value = 'path/to/file.xls'

            # and:
            submission_id = '63fde9'
            submission_url = "https://sample.com/submission/%s" % (submission_id)
            create_submission.return_value = submission_url

            # given:
            init_ingest_api.return_value = None
            uuid = 'dc54c78c-8556-4750-b527-b35b26645e39'
            get_object_uuid.return_value = uuid

            # when:
            response = self.client.post('/api_upload', headers={'Authorization': 'auth'})

            # then:
            self.assertEqual(201, response.status_code)

            # and: assert correct JSON details
            self.assertTrue(response.data)
            response_json = json.loads(response.data)
            response_details = response_json['details']
            self.assertEqual(submission_url, response_details['submission_url'])
            self.assertEqual(uuid, response_details['submission_uuid'])
            self.assertEqual(uuid, response_details['display_uuid'])
            self.assertEqual(submission_id, response_details['submission_id'])