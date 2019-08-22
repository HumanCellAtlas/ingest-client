import logging

from ingest.exporter.metadata import MetadataResource
from ingest.exporter.exceptions import FileDuplication
from typing import Optional
from ingest.api.ingestapi import IngestApi
from ingest.api.stagingapi import StagingApi

logger = logging.getLogger(__name__)


class StagingInfo:

    def __init__(self, staging_area_uuid: str, file_name: str, metadata_uuid: str, cloud_url: str):
        self.staging_area_uuid = staging_area_uuid
        self.file_name = file_name
        self.metadata_uuid = metadata_uuid
        self.cloud_url = cloud_url


class StagePersistInfo:

    def __init__(self, staging_area_uuid: str, file_name: str):
        self.staging_area_uuid = staging_area_uuid
        self.file_name = file_name


class StagingJob:
    def __init__(self, staging_area_uuid: str, file_name: str, create_date: str, cloud_url: Optional[str]):
        self.staging_area_uuid = staging_area_uuid
        self.file_name = file_name
        self.cloud_url = cloud_url
        self.create_date = create_date


class StagingJobTracker:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def delete_staging_jobs(self, staging_area_uuid: str):
        self.ingest_client.delete_staging_jobs(staging_area_uuid)

    def save(self, stage_persist_info: StagePersistInfo):
        pass

    def get_staging_job(self, staging_area_uuid: str, file_name: str) -> StagingJob:
        # return self.ingest_client.get_job(...)
        pass

    def staging_job_is_complete(self, staging_area_uuid: str, file_name: str) -> bool:
        return self.get_staging_job(staging_area_uuid, file_name).cloud_url is not None

    def complete_staging_job(self, staging_area_uuid: str, file_name: str, cloud_url: str):
        # return self.ingest_client.patch_job(...)
        pass


class StagingService:

    def __init__(self, staging_client: StagingApi, staging_job_tracker: StagingJobTracker):
        self.staging_client = staging_client
        self.staging_job_tracker = staging_job_tracker

    def staging_area_exists(self, staging_area_uuid: str) -> bool:
        return self.staging_client.hasStagingArea(staging_area_uuid)

    def get_staging_info(self, staging_area_uuid, metadata_resource: MetadataResource) -> StagingInfo:
        file_name = metadata_resource.get_staging_file_name()
        metadata_uuid = metadata_resource.uuid

        staging_job = self.staging_job_tracker.get_staging_job(staging_area_uuid, file_name)
        if staging_job.cloud_url:
            return StagingService._staging_info_for_complete_job(staging_job, metadata_uuid)
        else:
            file_description = self.staging_client.getFile(staging_area_uuid, file_name)
            self.staging_job_tracker.complete_staging_job(staging_area_uuid, file_name, file_description.url)
            return StagingInfo(staging_area_uuid, file_name, metadata_resource.uuid, file_description.url)

    @staticmethod
    def _staging_info_for_complete_job(staging_job: StagingJob, metadata_uuid: str) -> StagingInfo:
        if not staging_job.cloud_url:
            raise Exception()
        else:
            return StagingInfo(staging_job.staging_area_uuid, staging_job.file_name, metadata_uuid, staging_job.cloud_url)

    def stage_metadata(self, staging_area_uuid, metadata_resource: MetadataResource) -> StagingInfo:
        staging_file_name = metadata_resource.get_staging_file_name()

        try:
            self.staging_job_tracker.save(StagePersistInfo(staging_area_uuid, staging_file_name))
            formatted_type = f'metadata/{metadata_resource.metadata_type}'
            file_description = self.staging_client.stageFile(staging_area_uuid, staging_file_name,
                                                             metadata_resource.to_bundle_metadata(),
                                                             formatted_type)
        except FileDuplication as file_duplication:
            logger.warning(str(file_duplication))
            file_description = self.staging_client.getFile(staging_area_uuid, staging_file_name)

        staging_info = StagingInfo(staging_area_uuid, staging_file_name, metadata_resource.uuid, file_description.url)
        return staging_info

    def cleanup_staging_area(self, staging_area_uuid):
        self.staging_client.deleteStagingArea(staging_area_uuid)
        self.staging_job_tracker.delete_staging_jobs(staging_area_uuid)
