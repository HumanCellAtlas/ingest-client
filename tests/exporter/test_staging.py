from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.exceptions import FileDuplication
from ingest.exporter.metadata import MetadataResource, MetadataProvenance
from ingest.exporter.staging import StagingInfo, StagingService


class StagingServiceTest(TestCase):

    def test_stage_metadata(self):
        # given:
        staging_area_uuid = '2ba4f337-59f7-447c-8ef7-87f4d756a8b1'
        file_name = 'donor_organism_942f1908-8f75-4edd-90be-db1c44c12f62.json'
        content = '{"description": "test"}'
        content_type = 'metadata/biomaterial'

        # and:
        staging_client = Mock(name='staging_client')
        cloud_url = 'http://path.to/file_123'
        file_description = FileDescription(['chksUm'], 'biomaterial', file_name, 1024, cloud_url)
        staging_client.stageFile = Mock(return_value=file_description)
        staging_info_repository = Mock(name='staging_info_repository')

        # when:
        staging_service = StagingService(staging_client, staging_info_repository)
        staging_info = staging_service.stage_metadata(staging_area_uuid, file_name, content,
                                                      content_type)

        # then:
        self.assertIsNotNone(staging_info)
        self.assertEqual(staging_area_uuid, staging_info.staging_area_uuid)
        self.assertEqual(file_name, staging_info.file_name)
        self.assertEqual(cloud_url, staging_info.cloud_url)

        # and:
        staging_client.stageFile.assert_called_once_with(staging_area_uuid, file_name, content,
                                                         content_type)
        self._assert_staging_info_saved(staging_info_repository, file_name, staging_area_uuid)

    def _assert_staging_info_saved(self, staging_info_repository, file_name, staging_area_uuid):
        # and:
        call_list = staging_info_repository.save.call_args_list
        self.assertEqual(1, len(call_list), 'Save should have been called once.')
        call_args, _ = call_list[0]
        persisted_info, *_ = call_args

        # and: verify bare minimum correct staging info
        self.assertEqual(file_name, persisted_info.file_name)
        self.assertEqual(staging_area_uuid, persisted_info.staging_area_uuid)

    def test_stage_metadata_file_already_exists(self):
        # given:
        staging_info_repository = Mock(name='staging_info_repository')
        staging_info_repository.save = Mock(side_effect=FileDuplication())

        # and:
        staging_area_uuid = 'aa87cde5-a2de-424f-8f8e-40101af3f726'
        staging_client = Mock(name='staging_client')

        # when:
        staging_service = StagingService(staging_client, staging_info_repository)
        staging_service.stage_metadata(staging_area_uuid, 'duplicate.json',
                                       '{"text": "test"}', 'file')

        # then:
        staging_client.stageFile.assert_not_called()

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
        staging_service = StagingService(staging_client, Mock(name='staging_info_repository'))

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        staging_info = staging_service.stage_update(staging_area_uuid, metadata_resource)

        # then:
        self.assertTrue(type(staging_info) is StagingInfo,
                        'stage_update should return StagingInfo.')
        self.assertEqual(staging_area_uuid, staging_info.staging_area_uuid)
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(file_description.name, staging_info.file_name)
        self.assertEqual(file_description.url, staging_info.cloud_url)

        # and:
        staging_client.stageFile.assert_called_once_with(staging_area_uuid,
                                                         metadata_resource.get_staging_file_name(),
                                                         metadata_resource.to_bundle_metadata(),
                                                         'metadata/biomaterial')
