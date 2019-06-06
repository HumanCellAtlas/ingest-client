from copy import deepcopy
from typing import Iterable, Dict

from ingest.api.dssapi import DssApi, DSSFileCreateRequest
from ingest.api.stagingapi import StagingApi, MetadataFileStagingRequest, FileDescription


class MetadataToBeBundled:
    def __init__(self, metadata_type, uuid, version, cloud_url, file_descriptor):
        self.metadata_type = metadata_type
        self.uuid = uuid
        self.version = version
        self.cloud_url = cloud_url
        self.file_descriptor = file_descriptor


class MetadataResource:

    def __init__(self, metadata_type=None, metadata_json=None, uuid=None, dcp_version=None):
        self.metadata_type = metadata_type
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version

    @staticmethod
    def from_dict(data: dict):
        uuid_object = data.get('uuid')
        uuid = uuid_object.get('uuid') if uuid_object else None
        return MetadataResource(data.get('entityType'), data.get('content'), uuid,
                                data.get('dcpVersion'))

    def get_staging_file_name(self):
        return f'{self.uuid}.{self.dcp_version}.json'


class Bundle:

    def __init__(self, source={}):
        self._source = deepcopy(source)
        self._prepare_file_map()

    def _prepare_file_map(self):
        bundle_files = self._source.get('files')
        if not bundle_files:
            bundle_files = []
        self._file_map = {file.get('uuid'): file for file in bundle_files}

    def get_file(self, uuid):
        return self._file_map.get(uuid)

    def update_file(self, metadata_resource):
        pass


class MetadataService:

    def __init__(self, ingest_client):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)


class StagedMetadataResource:
    def __init__(self, metadata_resource: MetadataResource, staged_file_description: FileDescription):
        self.metadata_resource = metadata_resource
        self.staged_file_description = staged_file_description


class DSSMetadataFileResource:
    def __init__(self, dss_file_resource: dict, uuid: str, metadata_resource: MetadataResource):
        self.dss_file_resource = dss_file_resource
        self.uuid = uuid
        self.metadata_resource = metadata_resource


class StagingService:

    def __init__(self, staging_client):
        self.staging_client = staging_client

    def stage_update(self, staging_area_uuid,
                     metadata_resource: MetadataResource) -> FileDescription:
        return self.staging_client.stageFile(staging_area_uuid,
                                             metadata_resource.get_staging_file_name(),
                                             metadata_resource.metadata_json,
                                             metadata_resource.metadata_type)


class BundleUpdateService:

    def __init__(self, metadata_service: MetadataService, staging_service: StagingService):
        self.metadata_service = metadata_service
        self.staging_service = staging_service

        # FIXME keeping this for now just so I don't have to deal with missing var errors
        self.staging_client = None
        self.dss_client = None
        self.ingest_client = None

    # Note: tricky part here is updating bundles with the correct file-name within the bundle.
    # To do that, we must know
    # what we named the file-to-be-updated and ensure to name its update with the same name.
    def update_bundle(self, update_submission: dict, bundle_uuid: str, updated_bundle_version: str,
                      metadata_urls: Iterable[str]):
        metadata_resources = [self.metadata_service.fetch_resource(url) for url in metadata_urls]

        staging_area_id = update_submission["stagingDetails"]["stagingAreaUuid"]["uuid"]
        staged_file_descriptions = [self.staging_service.stage_update(metadata) for metadata in
                        metadata_resources]

    # TODO remove this
    def _temporarily_keep_this(self, bundle_uuid, staged_metadata_resources,
                               updated_bundle_version):
        # get all DSS bundles and patch in the updated DSS files
        dss_bundle_resource = self.get_bundle(bundle_uuid)
        dss_metadata_files = self.dss_store_metadata_updates(staged_metadata_resources)
        updated_bundle_files = BundleUpdateService.generate_patched_bundle_files(dss_bundle_resource, dss_metadata_files)
        updated_bundle = self.create_bundle(bundle_uuid, updated_bundle_version, updated_bundle_files)
        return updated_bundle

    def stage_metadata_resources(self, metadata_resources: Iterable[MetadataResource], staging_area_id: str) -> Iterable[StagedMetadataResource]:
        result = list(map(lambda metadata_resource: BundleUpdateService._stage_metadata_resource(
            metadata_resource, staging_area_id, self.staging_client), metadata_resources))
        return result

    def dss_store_metadata_updates(self, staged_metadata_files: Iterable[StagedMetadataResource]) \
            -> Iterable[DSSMetadataFileResource]:
        result = list(map(lambda staged_metadata_file: BundleUpdateService._dss_store_update_file(
            staged_metadata_file, self.dss_client), staged_metadata_files))
        return result

    def get_bundle(self, bundle_uuid: str) -> Iterable[dict]:
        return BundleUpdateService._get_bundle(bundle_uuid, self.dss_client)

    def create_bundle(self, bundle_uuid: str, bundle_version: str, bundle_files: Iterable[dict]):
        return BundleUpdateService._create_bundle(bundle_uuid, bundle_version, bundle_files, self.dss_client)

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
        metadata_type = metadata_resource.metadata_type
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
        file_list = map(lambda dss_file: (dss_file.uuid, dss_file), dss_files)
        return dict(file_list)

    @staticmethod
    def generate_patched_bundle_files(bundle_resource, dss_metadata_update_files: Iterable[DSSMetadataFileResource]) -> Iterable[dict]:
        uuid_dss_file_map = BundleUpdateService.generate_uuid_dss_file_map(dss_metadata_update_files)
        patched_bundle = dict(bundle_resource)
        bundle_files = patched_bundle["files"]

        patched_bundle_files = []
        for bundle_file in bundle_files:
            bundle_file_uuid = bundle_file.get("uuid")
            if bundle_file_uuid in uuid_dss_file_map:
                bundle_file_name = bundle_file.get("name")
                updated_dss_file = uuid_dss_file_map[bundle_file_uuid]
                patched_bundle_files.append({
                    "indexed": True,
                    "name": bundle_file_name,
                    "uuid": bundle_file_uuid,
                    "content-type": updated_dss_file.metadata_resource,
                    "version": updated_dss_file.dss_file_resource.get("version")
                })
            else:
                patched_bundle_files.append(bundle_file)

        return patched_bundle_files
