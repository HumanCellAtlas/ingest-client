import logging

from ingest.exporter.metadata import MetadataResource
from ingest.exporter.exceptions import FileDuplication

logger = logging.getLogger(__name__)


class StagingInfo:

    def __init__(self, staging_area_uuid, file_name, metadata_uuid='', cloud_url=''):
        self.staging_area_uuid = staging_area_uuid
        self.file_name = file_name
        self.metadata_uuid = metadata_uuid
        self.cloud_url = cloud_url


class StagingService:

    def __init__(self, staging_client, staging_info_repository):
        self.staging_client = staging_client
        self.staging_info_repository = staging_info_repository

    def stage_metadata(self, staging_area_uuid, file_name, content, content_type):
        try:
            staging_info = StagingInfo(staging_area_uuid, file_name)
            self.staging_info_repository.save(staging_info)
            file_description = self.staging_client.stageFile(staging_area_uuid, file_name, content,
                                                             content_type)
            staging_info.cloud_url = file_description.url
            return staging_info
        except FileDuplication as file_duplication:
            logger.warning(file_duplication)

    def stage_update(self, staging_area_uuid,
                     metadata_resource: MetadataResource) -> StagingInfo:
        formatted_type = f'metadata/{metadata_resource.metadata_type}'
        file_description = self.staging_client.stageFile(staging_area_uuid,
                                                         metadata_resource.get_staging_file_name(),
                                                         metadata_resource.to_bundle_metadata(),
                                                         formatted_type)

        staging_info = StagingInfo(staging_area_uuid=staging_area_uuid,
                           metadata_uuid=metadata_resource.uuid, file_name=file_description.name,
                           cloud_url=file_description.url)
        self.staging_info_repository.save(staging_info)
        return staging_info
