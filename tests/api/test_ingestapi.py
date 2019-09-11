from unittest import TestCase
from unittest.mock import patch

import requests
from mock import MagicMock, Mock
from requests import HTTPError

from ingest.api.ingestapi import IngestApi

mock_ingest_api_url = "http://mockingestapi.com"
mock_submission_envelope_id = "mock-envelope-id"


class IngestApiTest(TestCase):
    def setUp(self):
        self.token_manager = MagicMock()
        self.token_manager.get_token = Mock(return_value='token')

    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    @patch('ingest.api.ingestapi.requests.post')
    def test_create_file(self, mock_requests_post, mock_create_session, mock_get_link):
        ingest_api = IngestApi(token_manager=self.token_manager)
        mock_get_link.return_value = 'url/sub/id/files'
        mock_requests_post.return_value.json.return_value = {'uuid': 'file-uuid'}
        mock_requests_post.return_value.status_code = requests.codes.ok
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {})
        self.assertEqual(file, {'uuid': 'file-uuid'})
        mock_requests_post.assert_called_with('url/sub/id/files/mock-filename',
                                              headers={'Content-type': 'application/json',
                                                       'Authorization': 'Bearer token'},
                                              json={'fileName': 'mock-filename', 'content': {}},
                                              params={})

    @patch('ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    @patch('ingest.api.ingestapi.requests.post')
    def test_create_file_conflict(self, mock_requests_post, mock_create_session, mock_get_link, mock_get_file):
        ingest_api = IngestApi(token_manager=self.token_manager)
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
        mock_requests_post.return_value.status_code = requests.codes.conflict
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {'attr2': 'value2'})
        self.assertEqual(file, 'response')
        mock_requests_post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json', 'Authorization': 'Bearer token'},
                                json={'content': {'attr': 'value', 'attr2': 'value2'}})

    @patch('ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    @patch('ingest.api.ingestapi.requests.post')
    def test_create_file_internal_server_error(self, mock_requests_post, mock_create_session, mock_get_link,
                                               mock_get_file):
        ingest_api = IngestApi(token_manager=self.token_manager)
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
        mock_requests_post.return_value.status_code = requests.codes.internal_server_error
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename,
                                      {'attr2': 'value2'})
        self.assertEqual(file, 'response')
        mock_requests_post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json', 'Authorization': 'Bearer token'},
                                json={'content': {'attr': 'value',
                                                  'attr2': 'value2'}})

    @patch('ingest.api.ingestapi.IngestApi.get_link_from_resource_url')
    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_get_submission_by_uuid(self, mock_create_session, mock_get_link):
        mock_get_link.return_value = 'url/{?uuid}'
        ingest_api = IngestApi(token_manager=self.token_manager)
        mock_create_session.return_value.get.return_value.json.return_value = {'uuid': 'submission-uuid'}
        submission = ingest_api.get_submission_by_uuid('uuid')
        self.assertEqual(submission, {'uuid': 'submission-uuid'})

    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_get_all(self, mock_create_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)

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

    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_get_related_entities_count(self, mock_create_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)

        mocked_responses = {
            'https://url/project/files': {
                "page": {
                    "size": 3,
                    "totalElements": 5,
                    "totalPages": 2,
                    "number": 1
                },
                "_embedded": {
                    "files": [
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                }
            }
        }

        mock_entity = {
            "_links": {
                "self": {
                    "href": "https://url/project/1"
                },
                "files": {
                    "href": "https://url/project/files",
                }
            }
        }

        mock_create_session.return_value.get = lambda url, headers: self._create_mock_response(
            url, mocked_responses)

        # when
        count = ingest_api.get_related_entities_count('files', mock_entity, 'files')
        self.assertEqual(count, 5)

    @patch('ingest.api.ingestapi.create_session_with_retry')
    def test_get_related_entities_count_no_pagination(self, mock_create_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)

        mocked_responses = {
            'https://url/project/files': {
                "_embedded": {
                    "files": [
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                }
            }
        }

        mock_entity = {
            "_links": {
                "self": {
                    "href": "https://url/project/1"
                },
                "files": {
                    "href": "https://url/project/files",
                }
            }
        }

        mock_create_session.return_value.get = lambda url, headers: self._create_mock_response(
            url, mocked_responses)

        # when
        count = ingest_api.get_related_entities_count('files', mock_entity, 'files')
        self.assertEqual(count, 4)

    @patch('ingest.api.ingestapi.create_session_with_retry')
    @patch('ingest.api.ingestapi.requests.post')
    def test_create_staging_job_success(self, mock_post, mock_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)
        ingest_api.get_staging_jobs_url = MagicMock(return_value='url')

        mock_post.return_value.json.return_value = {'staging-area-uuid': 'uuid'}
        mock_post.return_value.status_code = requests.codes.ok

        # when
        staging_job = ingest_api.create_staging_job('uuid', 'filename', 'metadata-uuid')

        self.assertEqual(staging_job, {'staging-area-uuid': 'uuid'})

    @patch('ingest.api.ingestapi.create_session_with_retry')
    @patch('ingest.api.ingestapi.requests.post')
    def test_create_staging_job_failure(self, mock_post, mock_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)
        ingest_api.get_staging_jobs_url = MagicMock(return_value='url')

        mock_post.return_value.json.return_value = {'staging-area-uuid': 'uuid'}
        mock_post.return_value.status_code = requests.codes.ok

        mock_post.return_value.raise_for_status = MagicMock(side_effect=HTTPError())
        # when
        with self.assertRaises(HTTPError):
            ingest_api.create_staging_job('uuid', 'filename', 'metadata_uuid')

    @staticmethod
    def _create_mock_response(url, mocked_responses):
        response = MagicMock()
        response.json.return_value = mocked_responses.get(url)
        response.raise_for_status = Mock()
        return response
