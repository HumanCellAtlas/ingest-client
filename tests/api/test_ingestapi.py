import json
from unittest import TestCase
from unittest.mock import patch

import requests
from mock import MagicMock, Mock

from ingest.api.ingestapi import IngestApi

mock_ingest_api_url = "http://mockingestapi.com"
mock_submission_envelope_id = "mock-envelope-id"


class IngestApiTest(TestCase):
    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_create_file(self,  mock_create_session, mock_get_link):
        ingest_api = IngestApi()
        mock_get_link.return_value = 'url/sub/id/files'
        mock_create_session.return_value.post.return_value.json.return_value = {'uuid': 'file-uuid'}
        mock_create_session.return_value.post.return_value.status_code = requests.codes.ok
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {})
        self.assertEqual(file, {'uuid': 'file-uuid'})
        mock_create_session.return_value.post\
            .assert_called_with('url/sub/id/files/mock-filename',
                                headers={'Content-type': 'application/json'},
                                json={'fileName': 'mock-filename', 'content': {}},
                                params={})

    @patch('ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_create_file_conflict(self, mock_create_session, mock_get_link, mock_get_file):
        ingest_api = IngestApi()
        mock_get_file.return_value = {
            '_embedded': {
                'files': [
                    {
                        'content': {'attr': 'value'},
                        '_links': {'self': {'href': 'existing-file-url'}}
                    }
                ]
            }
        }

        mock_get_link.return_value = 'url/sub/id/files'
        mock_create_session.return_value.patch.return_value.json.return_value = 'response'
        mock_create_session.return_value.post.return_value.status_code = requests.codes.conflict
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {'attr2': 'value2'})
        self.assertEqual(file, 'response')
        mock_create_session.return_value.post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json'},
                                json={'content': {'attr': 'value', 'attr2': 'value2'}})

    @patch('ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_create_file_conflict(self, mock_create_session, mock_get_link,
                                  mock_get_file):
        ingest_api = IngestApi()
        mock_get_file.return_value = {
            '_embedded': {
                'files': [
                    {
                        'content': None,
                        '_links': {'self': {'href': 'existing-file-url'}}
                    }
                ]
            }
        }

        mock_get_link.return_value = 'url/sub/id/files'
        mock_create_session.return_value.patch.return_value.json.return_value = 'response'
        mock_create_session.return_value.post.return_value.status_code = requests.codes.conflict
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {'attr': 'value'})
        self.assertEqual(file, 'response')
        mock_create_session.return_value.post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json'},
                                json={'content': {'attr': 'value'}})

    @patch('ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_create_file_internal_server_error(self, mock_create_session, mock_get_link,
                                  mock_get_file):
        ingest_api = IngestApi()
        mock_get_file.return_value = {
            '_embedded': {
                'files': [
                    {
                        'content': {'attr': 'value'},
                        '_links': {'self': {'href': 'existing-file-url'}}
                    }
                ]
            }
        }

        mock_get_link.return_value = 'url/sub/id/files'
        mock_create_session.return_value.patch.return_value.json.return_value = 'response'
        mock_create_session.return_value.post.return_value.status_code = requests.codes.internal_server_error
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename,
                                      {'attr2': 'value2'})
        self.assertEqual(file, 'response')
        mock_create_session.return_value.post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json'},
                                json={'content': {'attr': 'value',
                                                  'attr2': 'value2'}})

    @patch('ingest.api.ingestapi.IngestApi.get_link_from_resource_url')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_get_submission_by_uuid(self, mock_create_session, mock_get_link):
        mock_get_link.return_value = 'url/{?uuid}'
        ingest_api = IngestApi()
        mock_create_session.return_value.get.return_value.json.return_value = {'uuid': 'submission-uuid'}
        submission = ingest_api.get_submission_by_uuid('uuid')
        self.assertEqual(submission, {'uuid': 'submission-uuid'})

    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_get_all(self, mock_create_session):
        # given
        ingest_api = IngestApi()

        mocked_responses = {
            'url?page=0&size=3': {
                "page": {
                    "size": 3,
                    "totalElements": 5,
                    "totalPages": 2,
                    "number": 0
                },
                "_embedded": {
                    "bundleManifests": [
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                    "next": {
                        'href': 'url?page=1&size=3'
                    }
                }
            },
            'url?page=1&size=3': {
                "page": {
                    "size": 3,
                    "totalElements": 5,
                    "totalPages": 2,
                    "number": 1
                },
                "_embedded": {
                    "bundleManifests": [
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                }
            }
        }

        mock_create_session.return_value.get = lambda url, headers: self._create_mock_response(url, mocked_responses)

        # when
        entities = ingest_api.get_all('url?page=0&size=3', "bundleManifests")
        self.assertEqual(len(list(entities)), 5)

    @staticmethod
    def _create_mock_response(url, mocked_responses):
        response = MagicMock()
        response.json.return_value = mocked_responses.get(url)
        response.raise_for_status = Mock()
        return response
