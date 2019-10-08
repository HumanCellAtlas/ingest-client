from unittest import TestCase
from mock import Mock, MagicMock
from requests import HTTPError

from ingest.exporter.storage import StorageService, StorageJobManager, StorageJob, StorageJobExists, StorageFailed
from ingest.exporter.staging import StagingService
from ingest.api.ingestapi import IngestApi
from ingest.api.dssapi import DssApi
from ingest.exporter.metadata import MetadataResource


class StorageJobManagerTest(TestCase):

    def test_create_storage_job(self):
        mock_metadata_uuid = "85de7758-4de3-4bcc-9848-c3f06ead684b"
        mock_dcp_version = "2018-03-26T14:27:53.360Z"

        mock_ingest_client = Mock(name="ingest-client", spec_set=IngestApi)
        mock_storage_job_resource = {
            "metadataUuid": mock_metadata_uuid,
            "metadataDcpVersion": mock_dcp_version,
            "entityType": "bionicmaterial",
            "status": "valid",
            "_links": {"self": {"href": "mock-storage-job-url"}}
        }

        mock_ingest_client.create_storage_job = MagicMock(return_value=mock_storage_job_resource)

        storage_job_manager = StorageJobManager(mock_ingest_client)
        created_storage_job = storage_job_manager.create_storage_job(mock_metadata_uuid, mock_dcp_version)
        expected_storage = StorageJob.from_json(mock_storage_job_resource)

        mock_ingest_client.create_storage_job.assert_called_with(mock_metadata_uuid, mock_dcp_version)
        self.assertEqual(expected_storage, created_storage_job)

    def test_create_storage_job_when_already_exists(self):
        mock_metadata_uuid = "85de7758-4de3-4bcc-9848-c3f06ead684b"
        mock_dcp_version = "2018-03-26T14:27:53.360Z"

        mock_ingest_client = Mock(name="ingest-client", spec_set=IngestApi)
        mock_ingest_client.create_storage_job = MagicMock()
        mock_create_http_response = Mock()
        mock_create_http_response.status_code = 409
        mock_ingest_client.create_storage_job.side_effect = HTTPError(response=mock_create_http_response)

        storage_job_manager = StorageJobManager(mock_ingest_client)
        self.assertRaises(StorageJobExists, lambda: storage_job_manager.create_storage_job(mock_metadata_uuid, mock_dcp_version))


class StorageServiceTest(TestCase):

    def test_store(self):
        mock_metadata_uuid = "85de7758-4de3-4bcc-9848-c3f06ead684b"
        mock_dcp_version = "2018-03-26T14:27:53.360Z"
        mock_metadata_type = "bionicmaterial"
        mock_metadata_resource = MetadataResource(mock_metadata_type, {}, mock_metadata_uuid, mock_dcp_version, None)

        mock_storage_job = StorageJob("mock-storage_job_url", mock_metadata_uuid, mock_dcp_version, mock_metadata_type, "valid")
        mock_storage_job_manager = Mock(name="storage_job_manager", spec_set=StorageJobManager)
        mock_storage_job_manager.create_storage_job = MagicMock(return_value=mock_storage_job)
        mock_storage_job_manager.complete_storage_job = MagicMock(name="complete storage job spy")

        mock_dss_client = Mock(name="dss_client", spec_set=DssApi)
        mock_dss_url = "mock-dss-url"
        mock_dss_client.return_value.url = mock_dss_url
        mock_dss_file = {"url": "mock-file-url"}
        mock_dss_client.put_file_v2 = MagicMock(return_value=mock_dss_file)

        mock_staging_service = Mock(name="staging_service", spec_set=StagingService)
        mock_staging_area_uuid = "31de7758-4de3-4bcc-9848-c3f06ead684b"

        storage_service = StorageService(mock_storage_job_manager, mock_dss_client, mock_staging_service)
        stored_url = storage_service.store(mock_metadata_resource, mock_staging_area_uuid)

        mock_storage_job_manager.complete_storage_job.assert_called_with(mock_storage_job)
        self.assertEqual(mock_dss_file["url"], stored_url)

    def test_store_when_storage_job_already_exists(self):
        mock_metadata_uuid = "85de7758-4de3-4bcc-9848-c3f06ead684b"
        mock_dcp_version = "2018-03-26T14:27:53.360Z"
        mock_metadata_type = "bionicmaterial"
        mock_metadata_resource = MetadataResource(mock_metadata_type, {}, mock_metadata_uuid, mock_dcp_version, None)

        mock_storage_job_manager = Mock(name="storage_job_manager", spec_set=StorageJobManager)
        mock_storage_job_manager.create_storage_job = MagicMock(side_effect=StorageJobExists())

        mock_dss_client = Mock(name="dss_client", spec=DssApi)
        mock_dss_url = "mock-dss-url"
        mock_dss_client.url = mock_dss_url

        mock_staging_service = Mock(name="staging_service", spec_set=StagingService)
        mock_staging_area_uuid = "31de7758-4de3-4bcc-9848-c3f06ead684b"

        mock_storage_job = StorageJob("mock-storage_job_url", mock_metadata_uuid, mock_dcp_version, mock_metadata_type, "submitted")
        mock_storage_job_manager.find_storage_job = MagicMock(return_value=mock_storage_job)
        mock_storage_job_manager.get_storage_job = MagicMock(return_value=mock_storage_job)

        storage_service = StorageService(mock_storage_job_manager, mock_dss_client, mock_staging_service)

        stored_url = storage_service.store(mock_metadata_resource, mock_staging_area_uuid)
        expected_url = f'{mock_dss_url}/files/{mock_metadata_uuid}?version={mock_dcp_version}'
        self.assertEqual(expected_url, stored_url)

    def test_store_cleanup_when_existing_job_times_out(self):
        mock_metadata_uuid = "85de7758-4de3-4bcc-9848-c3f06ead684b"
        mock_dcp_version = "2018-03-26T14:27:53.360Z"
        mock_metadata_type = "bionicmaterial"
        mock_metadata_resource = MetadataResource(mock_metadata_type, {}, mock_metadata_uuid, mock_dcp_version, None)

        mock_storage_job_manager = Mock(name="storage_job_manager", spec_set=StorageJobManager)
        mock_storage_job_manager.create_storage_job = MagicMock(side_effect=StorageJobExists())

        mock_dss_client = Mock(name="dss_client", spec=DssApi)
        mock_dss_url = "mock-dss-url"
        mock_dss_client.url = mock_dss_url

        mock_staging_service = Mock(name="staging_service", spec_set=StagingService)
        mock_staging_area_uuid = "31de7758-4de3-4bcc-9848-c3f06ead684b"

        mock_storage_job = StorageJob("mock-storage_job_url", mock_metadata_uuid, mock_dcp_version, mock_metadata_type, "not_submitted")
        mock_storage_job_manager.find_storage_job = MagicMock(return_value=mock_storage_job)
        mock_storage_job_manager.get_storage_job = MagicMock(return_value=mock_storage_job)
        mock_storage_job_manager.delete_storage_job = MagicMock(name="delete storage job spy")

        storage_service = StorageService(mock_storage_job_manager, mock_dss_client, mock_staging_service)

        self.assertRaises(StorageFailed, lambda: storage_service.store(mock_metadata_resource, mock_staging_area_uuid))
        mock_storage_job_manager.delete_storage_job.assert_called_with("mock-storage_job_url")
