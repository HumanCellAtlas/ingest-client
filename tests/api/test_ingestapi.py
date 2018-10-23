from unittest import TestCase
from unittest.mock import patch

from mock import MagicMock

from ingest.api.ingestapi import IngestApi

import json

mock_ingest_api_url = "http://mockingestapi.com"
mock_submission_envelope_id = "mock-envelope-id"


class IngestApiTest(TestCase):

    def test_create_file(self):
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"
        file_create_url = submission_url + "/files/" + filename

        # mock the load_root()
        with patch('ingest.api.ingestapi.IngestApi.get_root_url') as mock_load_root:
            root_links = dict()
            root_links["file"] = {"href": api_url + "/files"}
            root_links["submissionEnvelopes"] = {"href": api_url + "/submissionEnvelopes"}
            mock_load_root.return_value = root_links

            ingestapi = IngestApi(api_url)
            ingestapi.submission_links[submission_url] = {
                'files': {
                    'href': submission_url + "/files"
                }
            }

            with patch('ingest.api.ingestapi.requests.post') as mock_post:

                def mock_post_side_effect(*args, **kwargs):
                    mock_response = {}
                    mock_response['json'] = lambda _self: {}
                    mock_response['text'] = "{}"
                    mock_response['raise_for_status'] = MagicMock()

                    if args[0] == file_create_url:
                        mock_response['status_code'] = 201
                    else:
                        mock_response['status_code'] = 404

                    return type("MockResponse", (), mock_response)()

                mock_post.side_effect = mock_post_side_effect
                ingestapi.createFile(submission_url, filename, "{}")

    def test_get_submission_by_uuid(self):
        api_url = mock_ingest_api_url
        mock_submission_uuid = "mock-submission-uuid"
        submissions_url = api_url + "/submissionEnvelopes"
        submission_search_uri = submissions_url + "/search"
        findByUuidRel = "findByUuid"
        findByUuidHref = submission_search_uri + "/findByUuidHref{?uuid}"

        ingestapi = IngestApi(api_url, dict())
        with patch('ingest.api.ingestapi.IngestApi._get_url_for_link') as mock_get_url_for_link:
            def mock_get_url_for_link_patch(*args, **kwargs):
                if args[0] == submission_search_uri and args[1] == findByUuidRel:
                    return findByUuidHref

            mock_get_url_for_link.side_effect = mock_get_url_for_link_patch

            with patch('ingest.api.ingestapi.requests.get') as mock_requests_get:
                def mock_get_side_effect(*args, **kwargs):
                    mock_response = {}
                    mock_response_payload = {}

                    if args[0] == submission_search_uri + "/findByUuidHref" \
                            and 'params' in kwargs \
                            and 'uuid' in kwargs['params'] \
                            and kwargs['params']['uuid'] == mock_submission_uuid:
                        mock_response['status_code'] = 200
                        mock_response_payload = {"uuid": {"uuid": mock_submission_uuid}}
                    else:
                        mock_response['status_code'] = 404

                    mock_response['json'] = lambda _self: mock_response_payload
                    mock_response['text'] = json.dumps(mock_response_payload)

                    def raise_for_status():
                        raise Exception("test failed")

                    mock_response['raise_for_status'] = lambda _self: raise_for_status()

                    return type("MockResponse", (), mock_response)()

                mock_requests_get.side_effect = mock_get_side_effect

                assert 'uuid' in ingestapi.getSubmissionByUuid(mock_submission_uuid)