from ingest.api.ingestapi import IngestApi
from ingest.exporter.bundle import BundleService
from ingest.exporter.metadata import MetadataService
from ingest.exporter.staging import StagingService


class SubmissionEnvelopeParseException(Exception):
    pass


class SubmissionEnvelope:

    def __init__(self, uuid, staging_area_uuid):
        self.uuid = uuid
        self.staging_area_uuid = staging_area_uuid

    @staticmethod
    def from_dict(source: dict):
        try:
            uuid = source['uuid']['uuid']
            staging_area_uuid = source['stagingDetails']['stagingAreaUuid']['uuid']
            return SubmissionEnvelope(uuid, staging_area_uuid)
        except (KeyError, TypeError) as e:
            raise SubmissionEnvelopeParseException(e)


class Exporter:

    def __init__(self, ingest_api: IngestApi, metadata_service: MetadataService,
                 staging_service: StagingService, bundle_service: BundleService):
        self.ingest_api = ingest_api
        self.metadata_service = metadata_service
        self.staging_service = staging_service
        self.bundle_service = bundle_service

    def export_update(self, submission_source: dict, bundle_uuid: str, metadata_urls: list,
                      update_version: str):
        bundle = self.bundle_service.fetch(bundle_uuid)
        submission = SubmissionEnvelope.from_dict(submission_source)
        staging_details = self._apply_metadata_updates(bundle, metadata_urls,
                                                       submission.staging_area_uuid)
        bundle.update_version(update_version)
        self.bundle_service.update(bundle, staging_details)
        manifest = bundle.generate_manifest(submission.uuid)
        self.ingest_api.create_bundle_manifest(manifest)

    def _apply_metadata_updates(self, bundle, metadata_urls, staging_area_uuid):
        staging_details = []
        for url in metadata_urls:
            metadata_resource = self.metadata_service.fetch_resource(url)
            staging_info = self.staging_service.stage_update(staging_area_uuid, metadata_resource)
            staging_details.append(staging_info)
            bundle.update_file(metadata_resource)
        return staging_details
