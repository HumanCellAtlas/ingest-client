import json
from unittest import TestCase

from mock import MagicMock

from ingest.api.ingestapi import IngestApi
from ingest.importer.submission import Submission, Entity, IngestSubmitter, EntityLinker, LinkedEntityNotFound, \
    InvalidLinkInSpreadsheet, MultipleProcessesFound, EntitiesDictionaries

import ingest.api.ingestapi


class SubmissionTest(TestCase):

    def test_new_submission(self):
        # given
        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = MagicMock(name='ingest_api')

        submission = Submission(mock_ingest_api, submission_url='submission_url')
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
        mock_ingest_api = MagicMock(name='ingest_api')
        mock_ingest_api.load_root = MagicMock()
        mock_ingest_api.createEntity = MagicMock(return_value=new_entity_mock_response)

        submission = Submission(mock_ingest_api, submission_url='url')

        entity = Entity(id='id', type='biomaterial', content={})
        entity = submission.add_entity(entity)

        self.assertEqual(new_entity_mock_response, entity.ingest_json)


def _create_spreadsheet_json():
    spreadsheet_json = {
        'project': {
            'dummy-project-id': {
                'content': {
                    'key': 'project_1'
                }
            }
        },
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
        'protocol': {
            'protocol_id_1': {
                'content': {
                    'key': 'protocol_1'
                }
            }
        }
    }

    return spreadsheet_json


class IngestSubmitterTest(TestCase):

    def test_submit(self):
        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = IngestApi()
        mock_ingest_api.load_root = MagicMock()
        new_entity_mock_response = {'key': 'value'}
        mock_ingest_api.createEntity = MagicMock(return_value=new_entity_mock_response)
        mock_ingest_api.createFile = MagicMock(return_value=new_entity_mock_response)
        mock_ingest_api.linkEntity = MagicMock()

        spreadsheet_json = _create_spreadsheet_json()

        mock_template_manager = MagicMock(name='template_manager')
        mock_template_manager.get_schema_url = MagicMock(return_value='url')

        submitter = IngestSubmitter(mock_ingest_api, mock_template_manager)
        submission = submitter.submit(spreadsheet_json, submission_url='url')

        self.assertTrue(submission)
        self.assertTrue(submission.get_entity('biomaterial', 'biomaterial_id_1').ingest_json)
        self.assertEqual('biomaterial_1', submission.get_entity('biomaterial', 'biomaterial_id_1').content['key'])


class EntitiesDictionariesTest(TestCase):

    def test_load(self):
        spreadsheet_json = _create_spreadsheet_json()
        entities_map = EntitiesDictionaries(spreadsheet_json)

        self.assertEqual(['project', 'biomaterial', 'file', 'protocol'], list(entities_map.get_entity_types()))

        self.assertTrue(entities_map.get_entity('biomaterial', 'biomaterial_id_1'))
        self.assertEqual({'key': 'biomaterial_1'}, entities_map.get_entity('biomaterial', 'biomaterial_id_1').content)
        self.assertEqual('biomaterial_id_1', entities_map.get_entity('biomaterial', 'biomaterial_id_1').id)
        self.assertEqual('biomaterial', entities_map.get_entity('biomaterial', 'biomaterial_id_1').type)

        self.assertTrue(entities_map.get_entity('biomaterial', 'biomaterial_id_2'))
        self.assertEqual(spreadsheet_json['biomaterial']['biomaterial_id_2']['links_by_entity'],
                         entities_map.get_entity('biomaterial', 'biomaterial_id_2').links_by_entity)

        self.assertEqual({'key': 'protocol_1'}, entities_map.get_entity('protocol', 'protocol_id_1').content)


class EntityLinkerTest(TestCase):

    def setUp(self):
        mocked_template_manager = MagicMock(name='template_manager')
        mocked_template_manager.get_schema_url = MagicMock(return_value='url')
        self.mocked_template_manager = mocked_template_manager

    def test_generate_direct_links_biomaterial_to_biomaterial_has_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                }
            }
        }

        entity_linker = EntityLinker(self.mocked_template_manager)
        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        output = entity_linker.process_links(entities_dictionaries)

        self._assert_equal_direct_links(expected_json, output)

    def _assert_equal_direct_links(self, expected_json, output):
        for entity_type, entities_dict in expected_json.items():
            for entity_id, entity_dict in entities_dict.items():
                expected_links = entities_dict[entity_id].get('direct_links')
                expected_links = expected_links if expected_links else []
                entity = output.get_entity(entity_type, entity_id)
                self.assertTrue(entity)

                for link in expected_links:
                    self.assertTrue(link in entity.direct_links, f'{json.dumps(link)} is not in direct links')


    def test_generate_direct_links_biomaterial_to_biomaterial_no_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                }
            }
        }

        entity_linker = EntityLinker(self.mocked_template_manager)
        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        output = entity_linker.process_links(entities_dictionaries)

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_file_no_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                }
            }
        }

        entity_linker = EntityLinker(self.mocked_template_manager)
        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        output = entity_linker.process_links(entities_dictionaries)

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_file_has_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                }
            }
        }

        entity_linker = EntityLinker(self.mocked_template_manager)
        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        output = entity_linker.process_links(entities_dictionaries)

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_biomaterial_has_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        }
                    ]
                }
            }
        }

        entity_linker = EntityLinker(self.mocked_template_manager)
        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        output = entity_linker.process_links(entities_dictionaries)

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_link_not_found_error(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }
                }
            }
        }

        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager)

        with self.assertRaises(LinkedEntityNotFound) as context:
            entity_linker.process_links(entities_dictionaries)

        self.assertEqual('biomaterial', context.exception.entity)
        self.assertEqual('biomaterial_id_1', context.exception.id)

    def test_generate_direct_links_invalid_spreadsheet_link(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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

        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager)

        with self.assertRaises(InvalidLinkInSpreadsheet) as context:
            entity_linker.process_links(entities_dictionaries)

        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual('file', context.exception.link_entity_type)

        self.assertEqual('biomaterial_id_1', context.exception.from_entity.id)
        self.assertEqual('file_id_1', context.exception.link_entity_id)


    def test_generate_direct_links_multiple_process_links(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
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

        entities_dictionaries = EntitiesDictionaries(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager)

        with self.assertRaises(MultipleProcessesFound) as context:
            entity_linker.process_links(entities_dictionaries)

        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual(['process_id_1', 'process_id_2'], context.exception.process_ids)
