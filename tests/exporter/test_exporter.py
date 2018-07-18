import json
import os
import copy
import unittest
import uuid

import requests

from mock import MagicMock
from mock import Mock
from mock import patch


from unittest import TestCase

from ingest.exporter.ingestexportservice import IngestExporter
import ingest.exporter.ingestexportservice as ingestexportservice
import ingest.api.stagingapi as stagingapi

BASE_PATH = os.path.dirname(__file__)


class TestExporter(TestCase):
    def setUp(self):
        self.longMessage = True
        pass

    @patch('ingest.api.dssapi.DssApi')
    def test_get_input_bundle(self, dss_api_constructor):
        # given:
        dss_api_constructor.return_value = MagicMock('dss_api')

        # and:
        exporter = IngestExporter()

        # and:
        exporter.ingest_api.getRelatedEntities = MagicMock(return_value=['bundle1', 'bundle2'])
        process = {}

        # when:
        input_bundle = exporter.get_input_bundle(process)

        # then:
        self.assertEqual('bundle1', input_bundle)

    @unittest.skip
    @patch('ingest.api.dssapi.DssApi')
    def test_upload_metadata_files(self, dss_api_constructor):
        # given:
        dss_api_constructor.return_value = MagicMock('dss_api')

        # and:
        exporter = IngestExporter()

        # and:
        file_desc = stagingapi.FileDescription('checksums', 'contentType', 'name', 'name', 'file_url')
        exporter.upload_file = MagicMock(return_value=file_desc)
        metadata_files_info = {
            'project': {
                'dss_filename': 'project.json',
                'dss_uuid': 'uuid',
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'biomaterial': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'process': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'protocol': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'file': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'links': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            }
        }

        # when:
        metadata_files = exporter.upload_metadata_files('sub_uuid', metadata_files_info)

        # then:
        for metadata_list in metadata_files_info.values():
            for metadata_file in metadata_list:
                self.assertEqual(metadata_file['dss_uuid'], 'uuid')
                self.assertEqual(metadata_file['upload_file_url'], 'file_url')

    @patch('ingest.api.dssapi.DssApi')
    def test_upload_metadata_files_error(self, dss_api_constructor):
        # given:
        dss_api_constructor.return_value = MagicMock('dss_api')

        # and:
        exporter = IngestExporter()

        # and:
        exporter.upload_file = Mock(side_effect=Exception('test upload file error'))
        metadata_files_info = {
            'project': {
                'dss_filename': 'project.json',
                'dss_uuid': 'uuid',
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'biomaterial': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'process': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'protocol': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'file': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            },
            'links': {
                'dss_uuid': None,
                'content': {},
                'content_type': 'type',
                'upload_filename': 'filename'
            }
        }

        # when, then:
        with self.assertRaises(ingestexportservice.BundleFileUploadError) as e:
            metadata_files = exporter.upload_metadata_files('sub_uuid', metadata_files_info)

    @patch('ingest.api.dssapi.DssApi')
    def test_put_bundle_in_dss_error(self, dss_api_constructor):
        # given:
        dss_api_constructor.return_value = MagicMock('dss_api')

        # and:
        exporter = IngestExporter()

        # and:
        exporter.dss_api.put_bundle = Mock(side_effect=Exception('test create bundle error'))

        # when, then:
        with self.assertRaises(ingestexportservice.BundleDSSError) as e:
            metadata_files = exporter.put_bundle_in_dss('bundle_uuid', [])

    # mocks linked entities in the ingest API, attempts to build a bundle by crawling from an assay
    # process, asserts that the bundle created is equivalent to a known bundle
    @unittest.skip
    @patch('ingest.api.dssapi.DssApi')
    def test_create_bundle_manifest(self, dss_api_constructor):
        # given:
        dss_api_constructor.return_value = MagicMock('dss_api')

        class MockRequestResponse:
            def __init__(self, json_payload, status_code):
                self.payload = json_payload
                self.status_code = status_code
                self.text = json.dumps(json_payload)

            def json(self):
                return self.payload


        def mock_entities_url_to_file_dict():

            mock_entity_url_to_file_dict = dict()

            # analysis process
            mock_entity_url_to_file_dict["processes/mock-analysis-process-id"] = "/processes/mock_analysis_process.json"
            mock_entity_url_to_file_dict["processes/mock-analysis-process-id/derivedFiles"] = "/processes/mock_analysis_process_derived_files.json"
            mock_entity_url_to_file_dict["processes/mock-analysis-process-id/inputFiles"] = "/processes/mock_analysis_process_input_files.json"
            mock_entity_url_to_file_dict["processes/mock-analysis-process-id/inputBundleManifests"] = "/processes/mock_analysis_process_input_bundle_manifests.json"

            # input bundle manifests
            mock_entity_url_to_file_dict["bundleManifests/mock-input-bundle-manifest-id"] = "/processes/mock_bundle_manifest.json"

            # files
            mock_entity_url_to_file_dict["files/mock-fastq-read1-id"] = "/processes/mock_fastq_read1.json"
            mock_entity_url_to_file_dict["files/mock-fastq-read1-id/derivedByProcesses"] = "/files/mock_fastq_read1_derived_by_processes.json"
            mock_entity_url_to_file_dict["files/mock-fastq-read2-id"] = "/processes/mock_fastq_read2.json"
            mock_entity_url_to_file_dict["files/mock-fastq-read2-id/derivedByProcesses"] = "/files/mock_fastq_read2_derived_by_processes.json"

            # wrapper process(lib prep -> sequencing)
            mock_entity_url_to_file_dict["processes/mock-assay-process-id"] = "/processes/wrapper_process_lib_prep_and_sequencing.json"
            mock_entity_url_to_file_dict["processes/mock-assay-process-id/chainedProcesses"] = "/processes/wrapper_process_lib_prep_and_sequencing_chained_processes.json"
            mock_entity_url_to_file_dict["processes/mock-assay-process-id/inputBiomaterials"] = "/processes/wrapper_process_lib_prep_and_sequencing_input_biomaterial.json"
            mock_entity_url_to_file_dict["processes/mock-assay-process-id/derivedFiles"] = "/processes/wrapper_process_lib_prep_and_sequencing_derived_files.json"

            # lib prep process
            mock_entity_url_to_file_dict["processes/mock-lib-prep-process-id"] = "/processes/mock_lib_prep_process.json"
            mock_entity_url_to_file_dict["processes/mock-lib-prep-process-id/protocols"] = "/processes/mock_lib_prep_process_protocols.json"

            # sequencing process
            mock_entity_url_to_file_dict["processes/mock-sequencing-process-id"] = "/processes/mock_sequencing_process.json"
            mock_entity_url_to_file_dict["processes/mock-sequencing-process-id/protocols"] = "/processes/mock_sequencing_process_protocols.json"

            # cell suspension
            mock_entity_url_to_file_dict["biomaterials/mock-cell-suspension-id"] = "/biomaterials/mock_cell_suspension.json"
            mock_entity_url_to_file_dict["biomaterials/mock-cell-suspension-id/derivedByProcesses"] = "/biomaterials/mock_cell_suspension_derived_by_processes.json"

            # wrapper process(dissociation -> enrichment)
            mock_entity_url_to_file_dict["processes/mock-dissociation-enrichment-process-id"] = "/processes/wrapper_process_dissociation_and_enrichment.json"
            mock_entity_url_to_file_dict["processes/mock-dissociation-enrichment-process-id/chainedProcesses"] = "/processes/wrapper_process_dissociation_and_enrichment_chained_processes.json"
            mock_entity_url_to_file_dict["processes/mock-dissociation-enrichment-process-id/inputBiomaterials"] = "/processes/wrapper_process_dissociation_and_enrichment_input_biomaterial.json"
            mock_entity_url_to_file_dict["processes/mock-dissociation-enrichment-process-id/derivedBiomaterials"] = "/processes/wrapper_process_dissociation_and_enrichment_derived_biomaterial.json"

            # dissociation process
            mock_entity_url_to_file_dict["processes/mock-dissociation-process-id"] = "/processes/mock_dissociation_process.json"
            mock_entity_url_to_file_dict["processes/mock-dissociation-process-id/protocols"] = "/processes/mock_dissociation_process_protocols.json"

            # enrichment process
            mock_entity_url_to_file_dict["processes/mock-enrichment-process-id"] = "/processes/mock_encrichment_process.json"
            mock_entity_url_to_file_dict["processes/mock-enrichment-process-id/protocols"] = "/processes/mock_enrichment_process_protocols.json"

            # specimen
            mock_entity_url_to_file_dict["biomaterials/mock-specimen-id"] = "/biomaterials/mock_specimen.json"
            mock_entity_url_to_file_dict["biomaterials/mock-specimen-id/derivedByProcesses"] = "/biomaterials/mock_specimen_derived_by_processes.json"

            # sampling process
            mock_entity_url_to_file_dict["processes/mock-sampling-process-id"] = "/processes/mock_sampling_process.json"
            mock_entity_url_to_file_dict["processes/mock-sampling-process-id/inputBiomaterials"] = "/processes/mock_sampling_process_input_biomaterial.json"
            mock_entity_url_to_file_dict["processes/mock-sampling-process-id/derivedBiomaterials"] = "/processes/mock_sampling_process_derived_biomaterials.json"

            # donor
            mock_entity_url_to_file_dict["biomaterials/mock-donor-id"] = "/biomaterials/mock_donor.json"

            # project
            mock_entity_url_to_file_dict["projects/mock-project-id"] = "/projects/mock_project.json"

            return mock_entity_url_to_file_dict

        regular_requests_get = copy.deepcopy(requests.get)

        def mock_entities_retrieval(*args, **kwargs):
            test_ingest_dir = BASE_PATH + '/bundles/ingest-data'
            mock_entity_url_to_file_dict = mock_entities_url_to_file_dict()
            url = args[0]
            if 'mock-ingest-api' not in url:
                return regular_requests_get(*args, **kwargs)
            else: # mockville
                entity_relative_url = url.replace('http://mock-ingest-api/', '')
                if entity_relative_url in mock_entity_url_to_file_dict:
                    entity_file_location = mock_entity_url_to_file_dict[entity_relative_url]
                    with open(test_ingest_dir + entity_file_location, 'rb') as entity_file:
                        entity_json = json.load(entity_file)
                else: # don't have a mock for this entity; if it's a request for an empty input biomaterials/files/protocols, return a suitable empty _embedded
                    entity_json = {'_embedded' : dict(),
                                   '_links' :{
                                       'self' : {
                                           'href' : url
                                       }
                                   }}

                    if 'derivedByProcesses' in entity_relative_url or 'chainedProcesses' in entity_relative_url:
                        entity_json['_embedded'] = {'processes' : list()}
                    elif 'inputBiomaterials' in entity_relative_url or 'derivedBiomaterials' in entity_relative_url:
                        entity_json['_embedded'] = {'biomaterials' : list()}
                    elif 'inputBundleManifests' in entity_relative_url:
                        entity_json['_embedded'] = {'bundleManifests' : list()}
                    elif 'inputFiles' in entity_relative_url or 'derivedFiles' in entity_relative_url:
                        entity_json['_embedded'] = {'files' : list()}
                    elif 'protocols' in entity_relative_url:
                        entity_json['_embedded'] = {'protocols' : list()}
                    elif 'projects' in entity_relative_url:
                        with open(test_ingest_dir + '/projects/mock_project.json', 'rb') as project_file:
                            entity_json['_embedded'] = {'projects' : [json.load(project_file)]}
                    else:
                        raise Exception("Unknown resource in mock entities tests:" + url)

                return MockRequestResponse(entity_json, 200)

        # mock the calls to the ingest API for the entities in the bundle
        get_requests_mock = Mock()
        get_requests_mock.side_effect = mock_entities_retrieval
        requests.get = get_requests_mock

        exporter = ingestexportservice.IngestExporter()
        process_info = exporter.get_all_process_info('http://mock-ingest-api/processes/mock-assay-process-id')
        metadata_by_type = exporter.get_metadata_by_type(process_info)
        bundle_metadata_info = exporter.prepare_metadata_files(metadata_by_type)

        # assert that the contents of the bundle metadata info match that of the expected bundle
        self.assertEqual( # biomaterials...
            frozenset([biomaterial['hca_ingest']['document_id'] for biomaterial in bundle_metadata_info['biomaterial']['content']['biomaterials']]),
            frozenset([biomaterial['hca_ingest']['document_id'] for biomaterial in json_from_expected_bundle_file('assay/expected/Mouse Melanoma_biomaterial_bundle.json')['biomaterials']]))

        self.assertEqual( # processes...
            frozenset([process['hca_ingest']['document_id'] for process in bundle_metadata_info['process']['content']['processes']]),
            frozenset([process['hca_ingest']['document_id'] for process in json_from_expected_bundle_file('assay/expected/Mouse Melanoma_process_bundle.json')['processes']]))

        self.assertEqual( # protocols...
            frozenset([protocol['hca_ingest']['document_id'] for protocol in bundle_metadata_info['protocol']['content']['protocols']]),
            frozenset([protocol['hca_ingest']['document_id'] for protocol in json_from_expected_bundle_file('assay/expected/Mouse Melanoma_protocol_bundle.json')['protocols']]))

        self.assertEqual( # files...
            frozenset([file['hca_ingest']['document_id'] for file in bundle_metadata_info['file']['content']['files']]),
            frozenset([file['hca_ingest']['document_id'] for file in json_from_expected_bundle_file('assay/expected/Mouse Melanoma_file_bundle.json')['files']]))

        self.assertEqual( # links...
            frozenset([tuple(sorted(link.items())) for link in bundle_metadata_info['links']['content']['links']]),
            frozenset([tuple(sorted(link.items())) for link in json_from_expected_bundle_file('assay/expected/Mouse Melanoma_links_bundle.json')['links']])
        )

        self.assertEqual( # projects...
            bundle_metadata_info['project']['content']['hca_ingest']['document_id'],
            json_from_expected_bundle_file('assay/expected/Mouse Melanoma_project_bundle.json')['hca_ingest']['document_id']
        )

        # now run it on analysis
        process_info = exporter.get_all_process_info('http://mock-ingest-api/processes/mock-analysis-process-id')
        bundle_metadata_info = exporter.prepare_metadata_files(process_info)

        # assert that the contents of the bundle metadata info match that of the expected bundle
        self.assertEqual( # biomaterials...
            frozenset([biomaterial['hca_ingest']['document_id'] for biomaterial in bundle_metadata_info['biomaterial']['content']['biomaterials']]),
            frozenset([biomaterial['hca_ingest']['document_id'] for biomaterial in json_from_expected_bundle_file('analysis/expected/Mouse Melanoma_biomaterial_bundle.json')['biomaterials']]))

        self.assertEqual( # processes...
            frozenset([process['hca_ingest']['document_id'] for process in bundle_metadata_info['process']['content']['processes']]),
            frozenset([process['hca_ingest']['document_id'] for process in json_from_expected_bundle_file('analysis/expected/Mouse Melanoma_process_bundle.json')['processes']]))

        self.assertEqual( # protocols...
            frozenset([protocol['hca_ingest']['document_id'] for protocol in bundle_metadata_info['protocol']['content']['protocols']]),
            frozenset([protocol['hca_ingest']['document_id'] for protocol in json_from_expected_bundle_file('analysis/expected/Mouse Melanoma_protocol_bundle.json')['protocols']]))

        self.assertEqual( # files...
            frozenset([file['hca_ingest']['document_id'] for file in bundle_metadata_info['file']['content']['files']]),
            frozenset([file['hca_ingest']['document_id'] for file in json_from_expected_bundle_file('analysis/expected/Mouse Melanoma_file_bundle.json')['files']]))

        self.assertEqual( # links...
            frozenset([tuple(sorted(link.items())) for link in bundle_metadata_info['links']['content']['links']]),
            frozenset([tuple(sorted(link.items())) for link in json_from_expected_bundle_file('analysis/expected/Mouse Melanoma_links_bundle.json')['links']])
        )

        self.assertEqual( # projects...
            bundle_metadata_info['project']['content']['hca_ingest']['document_id'],
            json_from_expected_bundle_file('analysis/expected/Mouse Melanoma_project_bundle.json')['hca_ingest']['document_id']
        )


    def _create_entity_template(self):
        return {
            'submissionDate': '2018-03-14T09:53:02Z',
            'updateDate': '2018-03-14T09:53:02Z',
            'content': {},
            '_links': [],
            'events': [],
            'validationState': 'valid',
            'validationErrors': [],
            'uuid': {
                'uuid': '4674424e-3ab1-491c-8295-a68c7bb04b61'
            },
            'accession': 'accession123'
        }


def json_from_expected_bundle_file(relative_dir):
    # relative dir is relative to test/bundles
    with open(BASE_PATH + '/bundles/' + relative_dir, 'rb') as expected_bundle_file:
        return json.load(expected_bundle_file)
