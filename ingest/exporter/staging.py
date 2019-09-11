import logging
from typing import Optional
from os import environ
from time import sleep

import requests
from requests import HTTPError

from ingest.api.ingestapi import IngestApi
from ingest.api.stagingapi import StagingApi, FileUploadFailed
from ingest.exporter.exceptions import FileDuplication
from ingest.exporter.metadata import MetadataResource

logger = logging.getLogger(__name__)


wait_time_env = environ.get('STAGING_WAIT_TIME_MILLIS', '250')
STAGING_WAIT_TIME = float(wait_time_env) / 1000
if STAGING_WAIT_TIME < 0:
    raise ValueError('Staging wait time cannot be less than 0.')

wait_attempts_env = environ.get('STAGING_WAIT_ATTEMPTS', '5')
STAGING_WAIT_ATTEMPTS = int(wait_attempts_env)
if STAGING_WAIT_ATTEMPTS < 0:
    raise ValueError('Staging wait attempts cannot be less than 0.')


class StagingInfo:

    def __init__(self, staging_area_uuid, file_name, metadata_uuid='', cloud_url=''):
        self.staging_area_uuid = staging_area_uuid
        self.file_name = file_name
        self.metadata_uuid = metadata_uuid
        self.cloud_url = cloud_url


class StagingInfoRepository:

    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def save(self, staging_info: StagingInfo) -> StagingInfo:
        try:
            self.ingest_client.create_staging_job(staging_info.staging_area_uuid, staging_info.file_name, staging_info.metadata_uuid)
        except HTTPError as http_error:
            r = http_error.response
            if r.status_code == requests.codes.conflict:
                raise FileDuplication(staging_info.staging_area_uuid, staging_info.file_name)
            else:
                raise http_error
        else:
            return staging_info

    def find_one(self, staging_area_uuid, file_name) -> Optional[StagingInfo]:
        try:
            staging_job = self.ingest_client.find_staging_job(staging_area_uuid, file_name)
        except HTTPError as http_error:
            r = http_error.response
            if r.status_code == requests.codes.not_found:
                return None
            else:
                raise http_error
        else:
            staging_area_uuid = staging_job.get('stagingAreaUuid')
            file_name = staging_job.get('stagingAreaFileName')
            cloud_url = staging_job.get('stagingAreaFileUri')
            metadata_uuid = staging_job.get('metadataUuid')

            return StagingInfo(staging_area_uuid, file_name, cloud_url=cloud_url, metadata_uuid=metadata_uuid)

    def update(self, staging_info: StagingInfo) -> StagingInfo:
        staging_job = self.ingest_client.find_staging_job(staging_info.staging_area_uuid, staging_info.file_name)
        complete_job_url = staging_job['_links']['completeStagingJob']['href']
        self.ingest_client.complete_staging_job(complete_job_url, staging_info.cloud_url)
        return staging_info

    def delete(self, staging_info: StagingInfo) -> StagingInfo:
        staging_job = self.ingest_client.find_staging_job(staging_info.staging_area_uuid, staging_info.file_name)
        url = staging_job['_links']['self']['href']
        self.ingest_client.delete_staging_job(url)
        return staging_info

    def delete_staging_locks(self, staging_area_uuid: str):
        self.ingest_client.delete_staging_jobs(staging_area_uuid)


class StagingService:

    def __init__(self, staging_client: StagingApi, staging_info_repository: StagingInfoRepository):
        self.staging_client = staging_client
        self.staging_info_repository = staging_info_repository

    def get_staging_info(self, staging_area_uuid, metadata_resource: MetadataResource) -> Optional[StagingInfo]:
        file_name = metadata_resource.get_staging_file_name()
        staging_info = self.staging_info_repository.find_one(staging_area_uuid, file_name)
        remaining_attempts = STAGING_WAIT_ATTEMPTS
        while remaining_attempts and staging_info and not staging_info.cloud_url:
            sleep(STAGING_WAIT_TIME)
            staging_info = self.staging_info_repository.find_one(staging_area_uuid, file_name)
            remaining_attempts -= 1
        if staging_info and not staging_info.cloud_url:
            raise PartialStagingInfo(staging_info)
        return staging_info

    def stage_metadata(self, staging_area_uuid, metadata_resource: MetadataResource) -> Optional[StagingInfo]:
        staging_file_name = metadata_resource.get_staging_file_name()
        staging_info = StagingInfo(staging_area_uuid, staging_file_name, metadata_uuid=metadata_resource.uuid)
        try:
            self.staging_info_repository.save(staging_info)
            formatted_type = f'metadata/{metadata_resource.metadata_type}'
            bundle_metadata = metadata_resource.to_bundle_metadata()
            self._do_stage_metadata(staging_info, formatted_type, bundle_metadata)
        except FileDuplication as file_duplication:
            logger.warning(str(file_duplication))
            staging_info = self.get_staging_info(staging_area_uuid, metadata_resource)
            if not staging_info:
                raise StagingFailed(staging_area_uuid, staging_file_name)
        return staging_info

    def _do_stage_metadata(self, staging_info, formatted_type, bundle_metadata):
        try:
            # stageFile is assumed to do (sensible) internal retries (if necessary)
            file_description = self.staging_client.stageFile(staging_info.staging_area_uuid, staging_info.file_name,
                                                             bundle_metadata, formatted_type)
            staging_info.cloud_url = file_description.url
            self.staging_info_repository.update(staging_info)
        except FileUploadFailed as staging_failed:
            logging.error(str(staging_failed))
            self.staging_info_repository.delete(staging_info)
            raise staging_failed

    def staging_area_exists(self, staging_area_uuid: str) -> bool:
        return self.staging_client.hasStagingArea(staging_area_uuid)

    def cleanup_staging_area(self, staging_area_uuid):
        self.staging_client.deleteStagingArea(staging_area_uuid)
        self.staging_info_repository.delete_staging_locks(staging_area_uuid)


class PartialStagingInfo(Exception):

    def __init__(self, staging_info: StagingInfo):
        super(PartialStagingInfo, self).__init__('Unable to return StagingInfo with full details.')
        self.staging_info = staging_info


class StagingFailed(Exception):

    def __init__(self, staging_area_uuid, file_name):
        super(StagingFailed, self).__init__(f'Staging of file "{file_name}" on staging area '
                                            f'{staging_area_uuid} failed.')
