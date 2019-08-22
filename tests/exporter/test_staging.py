import logging
from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.exceptions import FileDuplication
from ingest.exporter.metadata import MetadataResource, MetadataProvenance
from ingest.exporter.staging import StagingInfo, StagingService

logging.disable(logging.CRITICAL)


class StagingServiceTest(TestCase):

    def test_stage_metadata(self):
        # given:
        metadata_resource = self._create_test_metadata_resource()

        # and:
        staging_client = Mock(name='staging_client')
        file_name = metadata_resource.get_staging_file_name()
        file_description = FileDescription(['chks0mz'], 'application/json', file_name, 1024,
                                           'http://domain.com/file.url')
        staging_client.stageFile = Mock(return_value=file_description)

        # and:
        staging_info_repository = Mock(name='staging_info_repository')
        staging_service = StagingService(staging_client, staging_info_repository)

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        staging_info = staging_service.stage_metadata(staging_area_uuid, metadata_resource)

        # then:
        self._assert_correct_staging_info(staging_info, staging_area_uuid, metadata_resource,
                                          file_description)
        staging_client.stageFile.assert_called_once_with(staging_area_uuid, file_name,
                                                         metadata_resource.to_bundle_metadata(),
                                                         'metadata/biomaterial')
        self._assert_staging_info_saved(staging_info_repository, file_name, staging_area_uuid)

    def _assert_correct_staging_info(self, staging_info, staging_area_uuid, metadata_resource,
                                     file_description):
        self.assertTrue(type(staging_info) is StagingInfo,
                        'stage_update should return StagingInfo.')
        self.assertEqual(staging_area_uuid, staging_info.staging_area_uuid)
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(file_description.name, staging_info.file_name)
        self.assertEqual(file_description.url, staging_info.cloud_url)

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
        metadata_resource = self._create_test_metadata_resource()
        file_name = metadata_resource.get_staging_file_name()

        # and:
        staging_area_uuid = '33302d25-b23c-4aeb-b56e-8b4493b60130'
        staging_info_repository = Mock(name='staging_info_repository')
        file_duplication = FileDuplication(staging_area_uuid, file_name)
        staging_info_repository.save = Mock(side_effect=file_duplication)

        # and:
        staging_client = Mock(name='staging_client')
        cloud_url = 'http://path/to/file_0.json'
        file_description = FileDescription(['cHks0mz'], 'metadata/process', file_name, 256,
                                           cloud_url)
        staging_client.getFile = Mock(return_value=file_description)

        # when:
        staging_service = StagingService(staging_client, staging_info_repository)
        staging_area_uuid = 'aa87cde5-a2de-424f-8f8e-40101af3f726'
        staging_info = staging_service.stage_metadata(staging_area_uuid, metadata_resource)

        # then:
        staging_client.stageFile.assert_not_called()
        staging_client.getFile.assert_called_once_with(staging_area_uuid, file_name)

        # and:
        self.assertIsNotNone(staging_info)
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(cloud_url, staging_info.cloud_url)

    def test_get_staging_info(self):
        # given:
        staging_info_repository = Mock(name='staging_info_repository')
        staging_service = StagingService(Mock(name='staging_client'), staging_info_repository)

        # and:
        staging_area_uuid = 'ac4b29e3-7522-417e-ae0f-d82874fb4b05'
        metadata_resource = self._create_test_metadata_resource()

        # and:
        file_name = metadata_resource.get_staging_file_name()
        cloud_url = 'http://domain.com/path/to/file.json'
        persisted_info = StagingInfo(staging_area_uuid, file_name, cloud_url=cloud_url)
        staging_info_repository.find_one = Mock(return_value=persisted_info)

        # when:
        staging_info = staging_service.get_staging_info(staging_area_uuid, metadata_resource)

        # then:
        self.assertIsNotNone(staging_info)
        self.assertEqual(staging_area_uuid, staging_info.staging_area_uuid)
        self.assertEqual(file_name, staging_info.file_name)
        self.assertEqual(cloud_url, staging_info.cloud_url)

        # and: just to ensure interface with repository is correct
        staging_info_repository.find_one.assert_called_once_with(staging_area_uuid, file_name)

    @staticmethod
    def _create_test_metadata_resource():
        provenance = MetadataProvenance('831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370', 'a submission date',
                                        'an update date', 1, 1)
        return MetadataResource(metadata_type='biomaterial',
                                uuid='831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370',
                                metadata_json={'description': 'test'},
                                dcp_version='4.2.1',
                                provenance=provenance)
