from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.metadata import MetadataResource
from ingest.exporter.staging import StagingInfo, StagingService


class StagingServiceTest(TestCase):

    def test_stage_update(self):
        # given:
        metadata_resource = MetadataResource(metadata_type='donor_organism',
                                             uuid='831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370',
                                             metadata_json={'description': 'test'},
                                             dcp_version='4.2.1')

        # and:
        staging_client = Mock(name='staging_client')
        file_description = FileDescription(['chks0mz'], 'application/json', 'file.name', 1024,
                                                'http://domain.com/file.url')
        staging_client.stageFile = Mock(return_value=file_description)

        # and:
        staging_service = StagingService(staging_client)

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        staging_file_name = 'file_21.json'
        staging_info = staging_service.stage_update(staging_area_uuid, staging_file_name,
                                                    metadata_resource)

        # then:
        self.assertTrue(type(staging_info) is StagingInfo,
                        'stage_update should return StagingRecord.')
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(file_description.name, staging_info.file_name)
        self.assertEqual(file_description.url, staging_info.cloud_url)

        # and:
        staging_client.stageFile.assert_called_once_with(staging_area_uuid,
                                                         staging_file_name,
                                                         metadata_resource.metadata_json,
                                                         metadata_resource.metadata_type)