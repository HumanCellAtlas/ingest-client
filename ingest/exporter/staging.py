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

    def stage_metadata(self, staging_area_uuid, metadata_resource: MetadataResource) -> StagingInfo:
        try:
            staging_info = StagingInfo(staging_area_uuid, metadata_resource.get_staging_file_name())
            self.staging_info_repository.save(staging_info)
            formatted_type = f'metadata/{metadata_resource.metadata_type}'
            file_description = self.staging_client.stageFile(staging_area_uuid,
                                                             metadata_resource.get_staging_file_name(),
                                                             metadata_resource.to_bundle_metadata(),
                                                             formatted_type)

            staging_info.metadata_uuid = metadata_resource.uuid
            staging_info.cloud_url = file_description.url
            return staging_info
        except FileDuplication as file_duplication:
            logger.warning(file_duplication)
