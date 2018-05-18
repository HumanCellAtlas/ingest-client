from unittest import TestCase

from mock import MagicMock

from ingest.api.ingestapi import IngestApi
from ingest.importer.submission import Submission, Entity, IngestSubmitter, EntityLinker, LinkedEntityNotFound, \
    InvalidLinkInSpreadsheet, MultipleProcessesFound

import ingest.api.ingestapi


class SubmissionTest(TestCase):

    def test_new_submission(self):
        # given
        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()
        mock_ingest_api.createSubmission = lambda token: 'submission_url'

        submission = Submission(mock_ingest_api, token='token')
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

        submission = Submission(mock_ingest_api, token='token')

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
        mock_ingest_api.createFile = MagicMock(return_value=new_entity_mock_response)
        mock_ingest_api.linkEntity = MagicMock()

        spreadsheet_json = self._create_spreadsheet_json()

        mock_template_manager = MagicMock(name='template_manager')
        mock_template_manager.get_schema_url = MagicMock(return_value='url')

        submitter = IngestSubmitter(mock_ingest_api, mock_template_manager)
        submission = submitter.submit(spreadsheet_json, token='token')

        self.assertTrue(submission)
        self.assertTrue(submission.get_entity('biomaterial', 'biomaterial_id_1').ingest_json)
        self.assertEqual('biomaterial_1', submission.get_entity('biomaterial', 'biomaterial_id_1').content['key'])

    def test_generate_entities_map(self):
        spreadsheet_json = self._create_spreadsheet_json()

        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()

        mock_template_manager = MagicMock(name='template_manager')

        submitter = IngestSubmitter(mock_ingest_api, mock_template_manager)
        entities_map = submitter.generate_entities_dict(spreadsheet_json)

        self.assertEqual(['biomaterial', 'file', 'process', 'protocol'], list(entities_map.keys()))
        self.assertEqual({'key': 'biomaterial_1'}, entities_map['biomaterial']['biomaterial_id_1'].content)
        self.assertEqual('biomaterial_id_1', entities_map['biomaterial']['biomaterial_id_1'].id)
        self.assertEqual('biomaterial', entities_map['biomaterial']['biomaterial_id_1'].type)
        self.assertEqual(spreadsheet_json['biomaterial']['biomaterial_id_2']['links_by_entity'], entities_map['biomaterial']['biomaterial_id_2'].links_by_entity)

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
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'process': ['process_id_1']
                    }
                },
                'biomaterial_id_3': {
                    'content': {
                        'key': 'biomaterial_3'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_2'],
                        'process': ['process_id_2']
                    }
                },
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'file_core': {
                            'file_name': 'file_name'
                        }
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_3']
                    }
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'links_by_entity': {
                        'protocol': ['protocol_id_1']
                    }
                },
                'process_id_2': {
                    'content': {
                        'key': 'process_2'
                    },
                    'links_by_entity': {
                        'protocol': ['protocol_id_1']
                    }
                },
                'process_id_3': {
                    'content': {
                        'key': 'process_3'
                    },
                    'links_by_entity': {
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


class EntityLinkerTest(TestCase):

    def setUp(self):
        mocked_template_manager = MagicMock(name='template_manager')
        mocked_template_manager.get_schema_url = MagicMock(return_value='url')
        self.mocked_template_manager = mocked_template_manager

    def test_generate_direct_links_biomaterial_to_biomaterial_has_process(self):
        # given
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
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    }
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                },
                'biomaterial_id_2': {
                    'content': {
                        'key': 'biomaterial_2'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        }
                    ]
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)
        output = entity_linker.generate_direct_links()

        self._assert_equal_direct_links(expected_json, output)

    def _assert_equal_direct_links(self, expected_json, output):
        for entity_type, entities_dict in expected_json.items():
            for entity_id, entity_dict in entities_dict.items():
                expected_links = entities_dict[entity_id].get('direct_links')
                expected_links = expected_links if expected_links else []
                actual_links = output[entity_type][entity_id].direct_links
                self.assertEqual(expected_links, actual_links)

    def test_generate_direct_links_biomaterial_to_biomaterial_no_process(self):
        # given
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
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'empty_process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                },
                'biomaterial_id_2': {
                    'content': {
                        'key': 'biomaterial_2'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'empty_process_id_1',
                            'relationship': 'derivedByProcesses'
                        }
                    ]
                }
            },
            'process': {
                'empty_process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)
        output = entity_linker.generate_direct_links()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_file_no_process(self):
        # given
        spreadsheet_json = {
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    }
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'empty_process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'empty_process_id_1',
                            'relationship': 'derivedByProcesses'
                        }
                    ]
                }
            },
            'process': {
                'empty_process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)
        output = entity_linker.generate_direct_links()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_file_has_process(self):
        # given
        spreadsheet_json = {
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    }
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    }
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        },
                    ]
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)
        output = entity_linker.generate_direct_links()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_biomaterial_has_process(self):
        # given
        spreadsheet_json = {
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }
                }
            },
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    }
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    }
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        }
                    ]
                },
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)
        output = entity_linker.generate_direct_links()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_link_not_found_error(self):
        # given
        spreadsheet_json = {
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)

        with self.assertRaises(LinkedEntityNotFound) as context:
            entity_linker.generate_direct_links()

        self.assertEqual('biomaterial', context.exception.entity)
        self.assertEqual('biomaterial_id_1', context.exception.id)

    def test_generate_direct_links_invalid_spreadsheet_link(self):
        # given
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1']
                    }
                }
            },
            'file':{
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)

        with self.assertRaises(InvalidLinkInSpreadsheet) as context:
            entity_linker.generate_direct_links()

        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual('file', context.exception.to_entity.type)

        self.assertEqual('biomaterial_id_1', context.exception.from_entity.id)
        self.assertEqual('file_id_1', context.exception.to_entity.id)

    def test_generate_direct_links_multiple_process_links(self):
        # given
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'links_by_entity': {
                        'process': ['process_id_1', 'process_id_2']
                    }
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    }
                },
                'process_id_2': {
                    'content': {
                        'key': 'process_2'
                    }
                }
            }
        }

        entities_dict_by_type = IngestSubmitter.generate_entities_dict(spreadsheet_json)

        entity_linker = EntityLinker(entities_dict_by_type, self.mocked_template_manager)

        with self.assertRaises(MultipleProcessesFound) as context:
            entity_linker.generate_direct_links()

        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual(['process_id_1', 'process_id_2'], context.exception.process_ids)
