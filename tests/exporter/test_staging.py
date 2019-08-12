from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.metadata import MetadataResource, MetadataProvenance
from ingest.exporter.staging import StagingInfo, StagingService


class StagingServiceTest(TestCase):

    def test_stage_metadata(self):
        # given:
        staging_client = Mock(name='staging_client')
        staging_service = StagingService(staging_client)

        # when:
        submission_uuid = '2ba4f337-59f7-447c-8ef7-87f4d756a8b1'
        upload_file_name = 'donor_organism_942f1908-8f75-4edd-90be-db1c44c12f62.json'
        staging_service.stage_metadata(submission_uuid, upload_file_name,
                                       '{"description": "test"}', 'metadata/biomaterial')

        # then:
        staging_client.stageFile.assert_called_once()


    def test_stage_update(self):
        # given:
        provenance = MetadataProvenance('831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370', 'a submission date',
                                        'an update date', 1, 1)
        metadata_resource = MetadataResource(metadata_type='biomaterial',
                                             uuid='831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370',
                                             metadata_json={'description': 'test'},
                                             dcp_version='4.2.1',
                                             provenance=provenance)

        # and:
        staging_client = Mock(name='staging_client')
        file_description = FileDescription(['chks0mz'], 'application/json', 'file.name', 1024,
                                           'http://domain.com/file.url')
        staging_client.stageFile = Mock(return_value=file_description)

        # and:
        staging_service = StagingService(staging_client)

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        staging_info = staging_service.stage_update(staging_area_uuid, metadata_resource)

        # then:
        self.assertTrue(type(staging_info) is StagingInfo,
                        'stage_update should return StagingRecord.')
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(file_description.name, staging_info.file_name)
        self.assertEqual(file_description.url, staging_info.cloud_url)

        # and:
        staging_client.stageFile.assert_called_once_with(staging_area_uuid,
                                                         metadata_resource.get_staging_file_name(),
                                                         metadata_resource.to_bundle_metadata(),
                                                         'metadata/biomaterial')
