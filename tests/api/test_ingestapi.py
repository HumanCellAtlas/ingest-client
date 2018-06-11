from unittest import TestCase
from unittest.mock import patch
from ingest.api.ingestapi import IngestApi


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

                    if args[0] == file_create_url:
                        mock_response['status_code'] = 201
                    else:
                        mock_response['status_code'] = 404

                    return type("MockResponse", (), mock_response)()

                mock_post.side_effect = mock_post_side_effect
                ingestapi.createFile(submission_url, filename, "{}")

