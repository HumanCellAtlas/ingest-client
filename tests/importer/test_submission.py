from unittest import TestCase

from mock import MagicMock

from ingest.api.ingestapi import IngestApi
from ingest.importer.submission import Submission, Entity, IngestSubmitter

import ingest.api.ingestapi


class SubmissionTest(TestCase):

    def test_new_submission(self):
        # given
        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()
        mock_ingest_api.createSubmission = lambda: 'submission_url'

        submission = Submission(mock_ingest_api)
        submission_url = submission.get_submission_url()

        self.assertEqual('submission_url', submission_url)

    def test_add_metadata(self):
        new_entity_mock_response = {
            'content': {},
            'submissionDate': '2018-05-08T10:17:49.476Z',
            'updateDate': '2018-05-08T10:17:57.254Z',
            'user': 'anonymousUser',
            'lastModifiedUser': 'anonymousUser',
            'uuid': {
                'uuid': '5a36689b-302b-40e4-bef1-837b47f0cb51'
            },
            'validationState': 'Draft',
            '_links': {
                'self': {
                    'href': 'http://api.ingest.dev.data.humancellatlas.org/protocols/5af1794d6a65a50007755b6a'
                },
                'protocol': {
                    'href': 'http://api.ingest.dev.data.humancellatlas.org/protocols/5af1794d6a65a50007755b6a',
                    'title': 'A single protocol'
                },
                'submissionEnvelopes': {
                    'href': 'http://api.ingest.dev.data.humancellatlas.org/protocols/5af1794d6a65a50007755b6a/submissionEnvelopes',
                    'title': 'Access or create new submission envelopes'
                }
            }
        }

        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()
        mock_ingest_api.load_root = MagicMock()
        mock_ingest_api.createSubmission = MagicMock(return_value='submission_url')
        mock_ingest_api.createEntity = MagicMock(return_value=new_entity_mock_response)

        submission = Submission(mock_ingest_api)

        entity = Entity(id='id', type='biomaterial', content={})
        entity = submission.add_entity(entity)

        self.assertEqual(new_entity_mock_response, entity.ingest_json)


class IngestSubmitterTest(TestCase):

    def test_submit(self):
        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()
        mock_ingest_api.load_root = MagicMock()
        mock_ingest_api.createSubmission = MagicMock(return_value='submission_url')
        new_entity_mock_response = {'key': 'value'}
        mock_ingest_api.createEntity = MagicMock(return_value=new_entity_mock_response)

        spreadsheet_json = self._create_spreadsheet_json()

        submitter = IngestSubmitter(mock_ingest_api)
        submission = submitter.submit(spreadsheet_json)

        self.assertTrue(submission)
        self.assertTrue(submission.get_entity('biomaterial', 'biomaterial_id_1').ingest_json)
        self.assertEqual('biomaterial_1', submission.get_entity('biomaterial', 'biomaterial_id_1').content['key'])

    def test_generate_entities_map(self):
        spreadsheet_json = self._create_spreadsheet_json()

        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()

        submitter = IngestSubmitter(mock_ingest_api)
        entities_map = submitter._generate_entities_map(spreadsheet_json)

        self.assertEqual(['biomaterial', 'file', 'process', 'protocol'], list(entities_map.keys()))
        self.assertEqual({'key': 'biomaterial_1'}, entities_map['biomaterial']['biomaterial_id_1'].content)
        self.assertEqual('biomaterial_id_1', entities_map['biomaterial']['biomaterial_id_1'].id)
        self.assertEqual('biomaterial', entities_map['biomaterial']['biomaterial_id_1'].type)
        self.assertEqual(spreadsheet_json['biomaterial']['biomaterial_id_2']['links'], entities_map['biomaterial']['biomaterial_id_2'].links)

        self.assertEqual({'key': 'protocol_1'}, entities_map['protocol']['protocol_id_1'].content)

    def _create_spreadsheet_json(self):
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    }
                },
                'biomaterial_id_2': {
                    'content': {
                        'key': 'biomaterial_2'
                    },
                    'links': {
                        'biomaterial': []
                    }
                },
                'biomaterial_id_3': {
                    'content': {
                        'key': 'biomaterial_3'
                    },
                    'links': {
                        'biomaterial': ['biomaterial_2'],
                        'process': ['process_id_2', 'process_id_3']
                    }
                },
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'links': {
                        'biomaterial': ['biomaterial_id_3']
                    }
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'links': {
                        'protocol': ['protocol_id_1']
                    }
                },
                'process_id_2': {
                    'content': {
                        'key': 'process_2'
                    },
                    'links': {
                        'protocol': ['protocol_id_1']
                    }
                },
                'process_id_3': {
                    'content': {
                        'key': 'process_3'
                    },
                    'links': {
                        'protocol': ['protocol_id_1']
                    }
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                }
            }
        }

        return spreadsheet_json

