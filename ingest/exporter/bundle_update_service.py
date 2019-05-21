from ingest.api.stagingapi import StagingApi, MetadataFileStagingRequest, FileDescription
from ingest.api.dssapi import DssApi, DSSFileCreateRequest
from ingest.api.ingestapi import IngestApi

from typing import Iterable, Dict


class MetadataToBeBundled:
    def __init__(self, metadata_type, uuid, version, cloud_url, file_descriptor):
        self.metadata_type = metadata_type
        self.uuid = uuid
        self.version = version
        self.cloud_url = cloud_url
        self.file_descriptor = file_descriptor


class MetadataResource:
    def __init__(self, metadata_resource, metadata_type, metadata_json, uuid, dcp_version):
        self.metadata_resource = metadata_resource
        self.metdata_type = metadata_type
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version


class StagedMetadataResource:
    def __init__(self, metadata_resource: MetadataResource, staged_file_description: FileDescription):
        self.metadata_resource = metadata_resource
        self.staged_file_description = staged_file_description


class DSSMetadataFileResource:
    def __init__(self, dss_file_resource: dict, uuid: str, metadata_resource: MetadataResource):
        self.dss_file_resource = dss_file_resource
        self.uuid = uuid
        self.metadata_resource = metadata_resource


class BundleUpdateService:
    def __init__(self, staging_client: StagingApi, dss_client: DssApi, ingest_client: IngestApi):
        self.staging_client = staging_client
        self.dss_client = dss_client
        self.ingest_client = ingest_client

    def update_bundle(self, update_submission: dict, bundle_uuid: str, updated_bundle_version: str, metadata_callbacks_to_update: Iterable[str]):
        """
        Note: tricky part here is updating bundles with the correct file-name within the bundle. To do that, we must know
        what we named the file-to-be-updated and ensure to name its update with the same name.

        :param update_submission:
        :param bundles_to_update:
        :param bundle_files_to_update:
        :return:
        """
        staging_area_id = BundleUpdateService.upload_area_id_for_submission(update_submission)

        # fetch the metadata_documents
        metadata_resources = self.fetch_and_parse_metadata(metadata_callbacks_to_update)

        # stage updates in the upload-service and store in dss
        staged_metadata_resources = self.stage_metadata_resources(metadata_resources, staging_area_id)
        dss_metadata_files = self.dss_store_metadata_updates(staged_metadata_resources)

        # get all DSS bundles and patch in the updated DSS files
        dss_bundle_resource = self.get_bundle(bundle_uuid)
        updated_bundle_files = BundleUpdateService.generate_patched_bundle_files(dss_bundle_resource, dss_metadata_files)
        updated_bundle = self.create_bundle(bundle_uuid, updated_bundle_version, updated_bundle_files)
        return updated_bundle

    def fetch_and_parse_metadata(self, metadata_urls: Iterable[str]) -> Iterable[MetadataResource]:
        return map(lambda metadata_resource: BundleUpdateService.parse_metadata(metadata_resource),
                   BundleUpdateService._fetch_metadata(metadata_urls, self.ingest_client))

    def stage_metadata_resources(self, metadata_resources: Iterable[MetadataResource], staging_area_id: str) -> Iterable[StagedMetadataResource]:
        return map(lambda metadata_resource: BundleUpdateService._stage_metadata_resource(metadata_resource, staging_area_id, self.staging_client),
                   metadata_resources)

    def dss_store_metadata_updates(self, staged_metadata_files: Iterable[StagedMetadataResource]) -> Iterable[DSSMetadataFileResource]:
        return map(lambda staged_metadata_file: BundleUpdateService._dss_store_update_file(staged_metadata_file, self.dss_client),
                   staged_metadata_files)

    def get_bundle(self, bundle_uuid: str) -> Iterable[dict]:
        return BundleUpdateService._get_bundle(bundle_uuid, self.dss_client)

    def create_bundle(self, bundle_uuid: str, bundle_version: str, bundle_files: Iterable[dict]):
        return BundleUpdateService._create_bundle(bundle_uuid, bundle_version, bundle_files, self.dss_client)

    @staticmethod
    def _fetch_metadata(metadata_callbacks: Iterable[str], ingest_client: IngestApi) -> Iterable[dict]:
        return map(lambda metadata_callback: ingest_client.get_entity_by_callback_link(metadata_callback),
                   metadata_callbacks)

    @staticmethod
    def _get_bundle(bundle_uuid: str, dss_client: DssApi):
        return dss_client.get_bundle(bundle_uuid)

    @staticmethod
    def _create_bundle(bundle_uuid: str, bundle_version: str, bundle_files: Iterable[dict], dss_client: DssApi) -> dict:
        return dss_client.put_bundle(bundle_uuid, bundle_version, list(bundle_files))

    @staticmethod
    def _stage_metadata_resource(metadata_resource: MetadataResource, staging_area_id: str, staging_client: StagingApi) -> StagedMetadataResource:
        metadata_stage_request = BundleUpdateService.generate_metadata_stage_request(staging_area_id, metadata_resource)
        staged_metadata = BundleUpdateService._stage_update(metadata_stage_request, staging_client)
        return StagedMetadataResource(metadata_resource, staged_metadata)

    @staticmethod
    def _stage_update(metadata_file_stage_request: MetadataFileStagingRequest, staging_client: StagingApi) -> FileDescription:
        return staging_client.stageFileRequest(metadata_file_stage_request)

    @staticmethod
    def _dss_store_update_file(staged_metadata_file: StagedMetadataResource, dss_client: DssApi) -> DSSMetadataFileResource:
        dss_file_create_request = BundleUpdateService.generate_dss_metadata_file_create_request(staged_metadata_file)
        return DSSMetadataFileResource(dss_client.create_file(dss_file_create_request), 
                                       dss_file_create_request.uuid, 
                                       staged_metadata_file.metadata_resource)

    @staticmethod
    def generate_metadata_stage_request(staging_area_id: str, metadata_resource: MetadataResource) -> MetadataFileStagingRequest:
        filename = f'{metadata_resource.uuid}.{metadata_resource.dcp_version}.json'
        metadata_json = metadata_resource.metadata_json
        metadata_type = metadata_resource.metdata_type
        return MetadataFileStagingRequest(staging_area_id, filename, metadata_json, metadata_type)

    @staticmethod
    def generate_dss_metadata_file_create_request(staged_metadata_file: StagedMetadataResource) -> DSSFileCreateRequest:
        uuid = staged_metadata_file.metadata_resource.uuid
        version = staged_metadata_file.metadata_resource.dcp_version
        source_cloud_url = staged_metadata_file.staged_file_description.url
        return DSSFileCreateRequest(uuid, version, source_cloud_url)

    @staticmethod
    def generate_uuid_dss_file_map(dss_files: Iterable[DSSMetadataFileResource]) -> Dict[str, DSSMetadataFileResource]:
        """
        Generates a hashmap of dss-file-uuid -> dss-file-resource
        :param dss_files:
        :return: a hashmap of dss-file-uuid -> dss-file-resource
        """
        return dict(map(lambda dss_file: (dss_file.uuid, dss_file), dss_files))

    @staticmethod
    def upload_area_id_for_submission(submission_resource: dict) -> str:
        return submission_resource["stagingDetails"]["stagingAreaUuid"]["uuid"]

    @staticmethod
    def parse_metadata(metadata_resource_json: dict) -> MetadataResource:
        metadata_type = metadata_resource_json["entityType"]
        metadata_json = metadata_resource_json["content"]
        uuid = metadata_resource_json["uuid"]["uuid"]
        dcp_version = metadata_resource_json["dcpVersion"]
        return MetadataResource(metadata_resource_json, metadata_type, metadata_json, uuid, dcp_version)

    @staticmethod
    def generate_patched_bundle_files(bundle_resource, dss_metadata_update_files: Iterable[DSSMetadataFileResource]) -> Iterable[dict]:
        uuid_dss_file_map = BundleUpdateService.generate_uuid_dss_file_map(dss_metadata_update_files)
        patched_bundle = dict(bundle_resource)
        bundle_files = patched_bundle["files"]

        patched_bundle_files = []
        for bundle_file in bundle_files:
            bundle_file_uuid = bundle_file["uuid"]
            if bundle_file_uuid in uuid_dss_file_map:
                bundle_file_name = bundle_file["name"]
                updated_dss_file = uuid_dss_file_map[bundle_file_uuid]
                patched_bundle_files.append({
                    "indexed": True,
                    "name": bundle_file_name,
                    "uuid": bundle_file_uuid,
                    "content-type": updated_dss_file.metadata_resource,
                    "version": updated_dss_file.dss_file_resource["version"]
                })
            else:
                patched_bundle_files.append(bundle_file)

        return patched_bundle_files
