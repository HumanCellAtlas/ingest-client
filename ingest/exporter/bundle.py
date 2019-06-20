import re
from copy import deepcopy

from ingest.api import utils
from ingest.api.dssapi import DssApi
from ingest.exporter.metadata import MetadataResource


_metadata_type_attr_map = {
    'biomaterial': 'fileBiomaterialMap',
    'file': 'fileFilesMap',
    'process': 'fileProcessMap',
    'project': 'fileProjectMap',
    'protocol': 'fileProtocolMap'
}


class BundleManifest:

    def __init__(self, bundleUuid=None, envelopeUuid={}, bundleVersion=None):
        self.bundleUuid = bundleUuid
        self.envelopeUuid = deepcopy(envelopeUuid)
        self.bundleVersion = bundleVersion
        self.dataFiles = []
        self.fileBiomaterialMap = {}
        self.fileProcessMap = {}
        self.fileFilesMap = {}
        self.fileProjectMap = {}
        self.fileProtocolMap = {}

    def add_bundle_file(self, metadata_type, entry: dict):
        attr_mapping = _metadata_type_attr_map.get(metadata_type)
        if attr_mapping:
            file_map = getattr(self, attr_mapping)
            file_map.update(entry)
        else:
            raise KeyError(f'Cannot map unknown metadata type [{metadata_type}].')


_CONTENT_TYPE_PATTERN = re.compile('.*"metadata/(?P<data_type>\\w+)".*')


class Bundle:

    def __init__(self, source={}):
        self._source = deepcopy(source)
        self._bundle = self._source.get('bundle')  # because bundle is nested in the root ¯\_(ツ)_/¯
        self._prepare_file_map()
        self.uuid = self._bundle.get('uuid') if self._bundle else None
        self.creator_uid = self._bundle.get('creator_uid') if self._bundle else None

    def _prepare_file_map(self):
        bundle_files = self._bundle.get('files') if self._bundle else None
        if not bundle_files:
            bundle_files = []
        self._file_map = {file.get('uuid'): file for file in bundle_files}

    @staticmethod
    def create(uuid: str, creator_uid: int, version: str):
        return Bundle(source={'bundle': {
            'uuid': uuid,
            'creator_uid': creator_uid,
            'version': version
        }})

    def get_version(self):
        return self._bundle.get('version')

    def get_file(self, uuid):
        return self._file_map.get(uuid)

    def get_files(self):
        return list(self._file_map.values())

    def count_files(self):
        return len(self._file_map)

    def update_version(self, version):
        self._bundle['version'] = version

    def update_file(self, metadata_resource: MetadataResource):
        target_file = self.get_file(metadata_resource.uuid)
        target_file['version'] = utils.to_dss_version(metadata_resource.dcp_version)
        # TODO content type needs to be complete HTTP-esque string not just "metadata/*"
        target_file['content-type'] = f'metadata/{metadata_resource.metadata_type}'

    def generate_manifest(self, envelope_uuid) -> BundleManifest:
        envelope_uuid_map = {'uuid': {'uuid': envelope_uuid}}
        manifest = BundleManifest(bundleUuid=self.uuid, envelopeUuid=envelope_uuid_map,
                                  bundleVersion=self.get_version())
        manifest.fileBiomaterialMap.update({uuid: [uuid] for uuid in self._file_map.keys()})
        for file_uuid, file in self._file_map.items():
            pattern_match = _CONTENT_TYPE_PATTERN.match(file.get('content-type'))
            if pattern_match:
                content_type = pattern_match.group('data_type')
                mapping = {file_uuid: [file_uuid]}
                manifest.add_bundle_file(content_type, mapping)
        return manifest


class BundleService:

    def __init__(self, dss_client: DssApi):
        self.dss_client = dss_client

    def fetch(self, uuid: str) -> Bundle:
        bundle_source = self.dss_client.get_bundle(uuid)
        return Bundle(source=bundle_source)

    def update(self, bundle: Bundle, staging_details: list):
        cloud_url_map = {info.metadata_uuid: info.cloud_url for info in staging_details}
        bundle_files = bundle.get_files()
        for uuid, cloud_url in cloud_url_map.items():
            file = bundle.get_file(uuid)
            self.dss_client.put_file(None, {'url': cloud_url, 'dss_uuid': uuid,
                                            'update_date': file.get('version')})
        self.dss_client.put_bundle(bundle.uuid, bundle.get_version(), bundle_files)
