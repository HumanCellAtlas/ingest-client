from ingest.api.ingestapi import IngestApi
from ingest.exporter.bundle import BundleService, BundleManifest
from ingest.exporter.metadata import MetadataService
from ingest.exporter.staging import StagingService


class Exporter:

    def __init__(self, ingest_api: IngestApi, metadata_service: MetadataService,
                 staging_service: StagingService, bundle_service: BundleService):
        self.ingest_api = ingest_api
        self.metadata_service = metadata_service
        self.staging_service = staging_service
        self.bundle_service = bundle_service

    def export_update(self, update_submission: dict, bundle_uuid: str, metadata_urls: list,
                      update_version: str):
        bundle = self.bundle_service.fetch(bundle_uuid)
        staging_details = []
        staging_area_uuid = update_submission['stagingDetails']['stagingAreaUuid']['uuid']
        for url in metadata_urls:
            metadata_resource = self.metadata_service.fetch_resource(url)
            staging_info = self.staging_service.stage_update(staging_area_uuid, metadata_resource)
            staging_details.append(staging_info)
            bundle.update_file(metadata_resource)
        bundle.update_version(update_version)
        self.bundle_service.update(bundle, staging_details)
        self.ingest_api.create_bundle_manifest(bundle.generate_manifest())
