from ingest.api.ingestapi import IngestApi
from ingest.api.dssapi import DssApi
from ingest.exporter.staging import StagingService
from ingest.exporter.metadata import MetadataResource

from ingest.api.utils import DSSVersion

from dataclasses import dataclass
from requests import HTTPError, codes
from time import sleep

from typing import Optional


class StorageJobExists(Exception):
    pass


class StorageJobTimeOut(Exception):
    pass


class StorageFailed(Exception):
    pass


@dataclass
class StorageJob:
    url: str
    metadata_uuid: str
    dcp_version: str
    entity_type: str
    is_completed: bool

    @staticmethod
    def from_json(storage_job_json) -> 'StorageJob':
        url = storage_job_json["_links"]["self"]["href"]
        metadata_uuid = storage_job_json["metadataUuid"]
        dcp_version = storage_job_json["metadataDcpVersion"]
        entity_type = storage_job_json["entityType"]
        is_completed = storage_job_json["isCompleted"]

        return StorageJob(url, metadata_uuid, dcp_version, entity_type, is_completed)


class StorageJobManager:

    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def create_storage_job(self, metadata_uuid, dcp_version) -> StorageJob:
        try:
            return StorageJob.from_json(self.ingest_client.create_storage_job(metadata_uuid, dcp_version))
        except HTTPError as e:
            if e.response.status_code == codes.conflict:
                raise StorageJobExists("storage job already exists") from e
            else:
                raise e

    def get_storage_job(self, storage_job_url) -> StorageJob:
        return StorageJob.from_json(self.ingest_client.get_storage_job(storage_job_url))

    def find_storage_job(self, metadata_uuid: str, dcp_version: str) -> Optional[StorageJob]:
        return StorageJob.from_json(self.ingest_client.find_storage_job(metadata_uuid, dcp_version))

    def complete_storage_job(self, storage_job: StorageJob) -> StorageJob:
        storage_job_url = storage_job.url
        return self.ingest_client.complete_storage_job(storage_job_url)

    def delete_storage_job(self, storage_job_url):
        return self.ingest_client.delete_staging_job(storage_job_url)


class StorageService:

    def __init__(self, storage_job_manager: StorageJobManager, dss_client: DssApi, staging_service: StagingService):
        self.storage_job_manager = storage_job_manager
        self.staging_service = staging_service
        self.dss_client = dss_client

    def store(self, metadata: MetadataResource, staging_area_uuid: str) -> str:
        return self._store(metadata, staging_area_uuid, 3)

    def _store(self, metadata: MetadataResource, staging_area_uuid: str, attempts: int) -> str:
        if attempts == 0:
            raise StorageFailed(f'Exhausted failed storage re-attempts (uuid: {metadata.uuid}, version: {metadata.dcp_version})')
        else:
            metadata_uuid = metadata.uuid
            dcp_version = metadata.dcp_version
            dss_version = DSSVersion(dcp_version)

            try:
                storage_job = self.storage_job_manager.create_storage_job(metadata_uuid, dcp_version)
                cloud_url = self.staging_service.stage_metadata(staging_area_uuid, metadata).cloud_url
                dss_file = self.dss_client.put_file_v2(metadata_uuid, dss_version, cloud_url)
                self.storage_job_manager.complete_storage_job(storage_job)
                dss_url = dss_file["url"]
                return dss_url
            except StorageJobExists:
                storage_job = self.storage_job_manager.find_storage_job(metadata_uuid, dcp_version)
                if storage_job:
                    try:
                        return self._wait_for_completed_storage_job(storage_job, 5, 1.5)
                    except StorageJobTimeOut:
                        self.storage_job_manager.delete_storage_job(storage_job.url)
                        return self._store(metadata, staging_area_uuid, attempts - 1)
                else:
                    return self._store(metadata, staging_area_uuid, attempts - 1)
            except Exception as e:
                raise StorageFailed() from e

    def _wait_for_completed_storage_job(self, storage_job: StorageJob, attempts: int, poll_period_seconds: float) -> str:
        if storage_job.is_completed:
            return self._get_file_dss_url(storage_job.metadata_uuid, storage_job.dcp_version)
        else:
            if attempts == 0:
                raise StorageJobTimeOut(f'Storage job at {storage_job.url} failed to complete in time (uuid: {storage_job.metadata_uuid}, version: {storage_job.dcp_version})')
            else:
                sleep(poll_period_seconds)
                storage_job = self.storage_job_manager.get_storage_job(storage_job.url)
                return self._wait_for_completed_storage_job(storage_job, attempts - 1, poll_period_seconds)

    def _get_file_dss_url(self, file_uuid: str, dcp_version: str):
        dss_base_url = self.dss_client.url
        return f'{dss_base_url}/files/{file_uuid}?version={dcp_version}'
