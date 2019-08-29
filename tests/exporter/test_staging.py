import logging
from copy import deepcopy
from unittest import TestCase

from mock import Mock, MagicMock

from ingest.api.stagingapi import FileDescription, StagingFailed
from ingest.exporter import staging
from ingest.exporter.exceptions import FileDuplication
from ingest.exporter.metadata import MetadataResource, MetadataProvenance
from ingest.exporter.staging import StagingInfo, StagingService, StagingInfoRepository, PartialStagingInfo

logging.disable(logging.CRITICAL)


class StagingInfoRepositoryTest(TestCase):
    def setUp(self) -> None:
        self.ingest_client = MagicMock(name='ingest_client')
        self.staging_info_repo = StagingInfoRepository(self.ingest_client)

    def tearDown(self) -> None:
        self.ingest_client = None
        self.staging_info_repo = None

    def test_save(self):
        staging_info = StagingInfo('staging-area-uuid', 'file-name', 'metadata-uuid', 'cloud-url')
        mock_save_response = MagicMock()
        mock_save_response.status_code = MagicMock(return_value=200)
        self.ingest_client.create_staging_job = MagicMock(return_value=mock_save_response)
        output = self.staging_info_repo.save(staging_info)
        self.ingest_client.create_staging_job.assert_called_once_with(staging_info.staging_area_uuid,
                                                                      staging_info.file_name)
        self.assertEqual(staging_info.staging_area_uuid, output.staging_area_uuid)
        self.assertEqual(staging_info.file_name, output.file_name)
        self.assertEqual(staging_info.metadata_uuid, output.metadata_uuid)

    def test_save_has_conflict(self):
        staging_info = StagingInfo('staging-area-uuid', 'file-name', 'metadata-uuid', 'cloud-url')
        mock_save_response = MagicMock()
        mock_save_response.status_code = 409
        self.ingest_client.create_staging_job = MagicMock(return_value=mock_save_response)

        with self.assertRaises(FileDuplication):
            self.staging_info_repo.save(staging_info)

    def test_find_one(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response_body = {
            'stagingAreaUuid': 'staging-area-uuid',
            'stagingAreaFileName': 'filename',
            'stagingAreaFileUri': 'cloudurl'
        }
        mock_response.json = MagicMock(return_value=mock_response_body)
        self.ingest_client.find_staging_job = MagicMock(
            return_value=mock_response)
        output = self.staging_info_repo.find_one('staging-area-uuid', 'filename')
        self.ingest_client.find_staging_job.assert_called_once_with('staging-area-uuid', 'filename')
        self.assertEqual(output.cloud_url, 'cloudurl')
        self.assertEqual(output.staging_area_uuid, 'staging-area-uuid')
        self.assertEqual(output.file_name, 'filename')

    def test_find_one_not_found(self):
        mock_response = MagicMock()
        mock_response.status_code = 404

        self.ingest_client.find_staging_job = MagicMock(return_value=mock_response)
        output = self.staging_info_repo.find_one('staging-area-uuid', 'filename')
        self.assertFalse(output)

    def test_find_one_with_status_exception(self):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(side_effect=Exception('test'))
        self.ingest_client.find_staging_job = MagicMock(return_value=mock_response)
        with self.assertRaises(Exception):
            self.staging_info_repo.find_one('staging-area-uuid', 'filename')

    def test_update(self):
        staging_info = StagingInfo('staging-area-uuid', 'file-name', 'metadata-uuid', 'cloud-url')
        staging_job = {'_links': {'completeStagingJob': {'href': 'complete_url'}}}
        self.ingest_client.find_staging_job = MagicMock(return_value=staging_job)
        self.ingest_client.complete_staging_job = MagicMock()
        output = self.staging_info_repo.update(staging_info)
        self.ingest_client.find_staging_job.assert_called_once_with(staging_info.staging_area_uuid,
                                                                    staging_info.file_name)
        self.ingest_client.complete_staging_job.assert_called_once_with('complete_url', staging_info.cloud_url)
        self.assertEqual(staging_info.cloud_url, output.cloud_url)

    def test_delete(self):
        staging_info = StagingInfo('staging-area-uuid', 'file-name', 'metadata-uuid', 'cloud-url')
        staging_job = {'_links': {'self': {'href': 'delete_url'}}}
        self.ingest_client.find_staging_job = MagicMock(return_value=staging_job)
        self.ingest_client.delete_staging_job = MagicMock()
        self.staging_info_repo.delete(staging_info)
        self.ingest_client.find_staging_job.assert_called_once_with(staging_info.staging_area_uuid,
                                                                    staging_info.file_name)
        self.ingest_client.delete_staging_job.assert_called_once_with('delete_url')


class StagingServiceTest(TestCase):

    def setUp(self):
        self.staging_client = Mock(name='staging_client')
        self.staging_info_repository = Mock(name='staging_info_repository')
        self.staging_service = StagingService(self.staging_client, self.staging_info_repository)

    def tearDown(self):
        self.staging_client = None
        self.staging_info_repository = None
        self.staging_service = None

    def test_stage_metadata(self):
        # given:
        metadata_resource = self._create_test_metadata_resource()

        # and:
        file_name = metadata_resource.get_staging_file_name()
        cloud_url = 'http://domain.com/file.url'
        file_description = FileDescription(['chks0mz'], 'biomaterial', file_name, 1024, cloud_url)
        self.staging_client.stageFile = Mock(return_value=file_description)

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        staging_info = self.staging_service.stage_metadata(staging_area_uuid, metadata_resource)

        # then:
        self._assert_staging_info_saved(self.staging_info_repository, file_name, staging_area_uuid)
        self._assert_correct_staging_info(staging_info, staging_area_uuid, metadata_resource, file_description)
        self.staging_client.stageFile.assert_called_once_with(staging_area_uuid, file_name,
                                                              metadata_resource.to_bundle_metadata(),
                                                              'metadata/biomaterial')
        self._assert_cloud_url_updated(cloud_url)

    def _assert_correct_staging_info(self, staging_info, staging_area_uuid, metadata_resource, file_description):
        self.assertTrue(type(staging_info) is StagingInfo, 'stage_update should return StagingInfo.')
        self.assertEqual(staging_area_uuid, staging_info.staging_area_uuid)
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(file_description.name, staging_info.file_name)
        self.assertEqual(file_description.url, staging_info.cloud_url)

    def _assert_staging_info_saved(self, staging_info_repository, file_name, staging_area_uuid):
        call_list = staging_info_repository.save.call_args_list
        self.assertEqual(1, len(call_list), 'Save should have been called once.')
        call_args, _ = call_list[0]
        persisted_info, *_ = call_args

        # verify bare minimum correct staging info
        self.assertEqual(file_name, persisted_info.file_name)
        self.assertEqual(staging_area_uuid, persisted_info.staging_area_uuid)

    def _assert_cloud_url_updated(self, cloud_url):
        update_call_list = self.staging_info_repository.update.call_args_list
        self.assertEqual(1, len(update_call_list), 'update should have been called exactly once.')
        call_args, _ = update_call_list[0]
        update_info, *_ = call_args
        self.assertEqual(cloud_url, update_info.cloud_url)

    def test_stage_metadata_file_already_exists(self):
        # given:
        metadata_resource = self._create_test_metadata_resource()
        metadata_uuid = metadata_resource.uuid
        file_name = metadata_resource.get_staging_file_name()

        # and:
        staging_area_uuid = '33302d25-b23c-4aeb-b56e-8b4493b60130'
        file_duplication = FileDuplication(staging_area_uuid, file_name)
        self.staging_info_repository.save = Mock(side_effect=file_duplication)

        # and:
        cloud_url = 'http://path/to/file_0.json'
        persistent_info = StagingInfo(staging_area_uuid, file_name, metadata_uuid=metadata_uuid, cloud_url=cloud_url)
        self.staging_info_repository.find_one = Mock(return_value=persistent_info)

        # when:
        staging_area_uuid = 'aa87cde5-a2de-424f-8f8e-40101af3f726'
        staging_info = self.staging_service.stage_metadata(staging_area_uuid, metadata_resource)

        # then:
        self.staging_client.stageFile.assert_not_called()

        # and:
        self.assertIsNotNone(staging_info)
        self.assertEqual(metadata_uuid, staging_info.metadata_uuid)
        self.assertEqual(cloud_url, staging_info.cloud_url)

        # and: ensure consistent interface
        self.staging_info_repository.update.assert_not_called()
        self.staging_info_repository.find_one.assert_called_once_with(staging_area_uuid, file_name)

    def test_stage_metadata_delete_info_on_failure(self):
        # given:
        staging_area_uuid = '4717a115-6701-4230-96f3-86c98fd2218d'
        metadata_resource = self._create_test_metadata_resource()
        file_name = metadata_resource.get_staging_file_name()

        # and:
        staging_failure = StagingFailed.formatted(staging_area_uuid, file_name)
        self.staging_client.stageFile = Mock(side_effect=staging_failure)

        # when:
        with self.assertRaises(StagingFailed) as context:
            self.staging_service.stage_metadata(staging_area_uuid, metadata_resource)

        # then:
        self.staging_info_repository.update.assert_not_called()
        self.assertEqual(staging_failure, context.exception)

        # and: partial Staging Info should be deleted
        self.staging_info_repository.delete.assert_called_once()
        call_args, _ = self.staging_info_repository.delete.call_args
        deleted_info, *_ = call_args
        self.assertEqual(staging_area_uuid, deleted_info.staging_area_uuid)
        self.assertEqual(file_name, deleted_info.file_name)

    def test_get_staging_info(self):
        # given:
        staging_area_uuid = 'ac4b29e3-7522-417e-ae0f-d82874fb4b05'
        metadata_resource = self._create_test_metadata_resource()

        # and:
        file_name = metadata_resource.get_staging_file_name()
        cloud_url = 'http://domain.com/path/to/file.json'
        persistent_info = StagingInfo(staging_area_uuid, file_name, cloud_url=cloud_url)
        self.staging_info_repository.find_one = Mock(return_value=persistent_info)

        # when:
        staging_info = self.staging_service.get_staging_info(staging_area_uuid, metadata_resource)

        # then:
        self.assertIsNotNone(staging_info)
        self.assertEqual(staging_area_uuid, staging_info.staging_area_uuid)
        self.assertEqual(file_name, staging_info.file_name)
        self.assertEqual(cloud_url, staging_info.cloud_url)

        # and: just to ensure interface with repository is correct
        self.staging_info_repository.find_one.assert_called_once_with(staging_area_uuid, file_name)
        self.staging_info_repository.update.assert_not_called()

    def test_get_staging_info_non_existent(self):
        # given:
        staging_area_uuid = '34b86b17-e186-45e0-9ad3-d4842b253ab7'
        metadata_resource = self._create_test_metadata_resource()
        self.staging_info_repository.find_one = Mock(return_value=None)

        # when:
        staging_info = self.staging_service.get_staging_info(staging_area_uuid, metadata_resource)

        # then:
        self.assertIsNone(staging_info)

    def test_get_staging_info_retry_for_missing_cloud_url(self):
        # given:
        staging.STAGING_WAIT_TIME = 0.01

        # and:
        staging_area_uuid = 'd4498a65-5b00-4424-8101-4e4a6a7e5382'
        metadata_resource = self._create_test_metadata_resource()

        # and:
        file_name = metadata_resource.get_staging_file_name()
        persistent_info = StagingInfo(staging_area_uuid, file_name)

        # and:
        cloud_url = 'http://this/leads/to/the_file_0.json'
        updated_info = deepcopy(persistent_info)
        updated_info.cloud_url = cloud_url

        # and:
        ordered_responses = [persistent_info, persistent_info, updated_info]
        staging.STAGING_WAIT_ATTEMPTS = len(ordered_responses) - 1
        self.staging_info_repository.find_one = Mock(side_effect=ordered_responses)

        # when:
        staging_info = self.staging_service.get_staging_info(staging_area_uuid, metadata_resource)

        # then:
        self.assertIsNotNone(staging_info)
        self.assertEqual(cloud_url, staging_info.cloud_url)

    def test_get_staging_info_exhaust_attempts(self):
        # given: alter wait parameters for testing
        staging.STAGING_WAIT_ATTEMPTS = 1
        staging.STAGING_WAIT_TIME = 0.01

        # and:
        staging_area_uuid = 'f98bf97d-0b43-4eb2-a463-0df1c0d48142'
        metadata_resource = self._create_test_metadata_resource()

        # and:
        staging_info = StagingInfo(staging_area_uuid, metadata_resource.get_staging_file_name())
        self.staging_info_repository.find_one = Mock(return_value=staging_info)

        # when:
        with self.assertRaises(PartialStagingInfo) as context:
            self.staging_service.get_staging_info(staging_area_uuid, metadata_resource)

        # then:
        self.assertEqual(staging_info, context.exception.staging_info)

    def test_get_staging_info_eventually_non_existent(self):
        # given:
        staging.STAGING_WAIT_TIME = 0.1

        # and:
        staging_area_uuid = '34b86b17-e186-45e0-9ad3-d4842b253ab7'
        metadata_resource = self._create_test_metadata_resource()

        # and:
        persistent_info = StagingInfo(staging_area_uuid, metadata_resource.get_staging_file_name())
        ordered_responses = [persistent_info, persistent_info, persistent_info, None]
        staging.STAGING_WAIT_ATTEMPTS = len(ordered_responses) - 1
        self.staging_info_repository.find_one = Mock(side_effect=ordered_responses)

        # when:
        staging_info = self.staging_service.get_staging_info(staging_area_uuid, metadata_resource)

        # then:
        self.assertIsNone(staging_info)

    @staticmethod
    def _create_test_metadata_resource():
        provenance = MetadataProvenance('831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370', 'a submission date', 'an update date',
                                        1, 1)
        return MetadataResource(metadata_type='biomaterial', uuid='831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370',
                                metadata_json={'description': 'test'}, dcp_version='4.2.1', provenance=provenance)
