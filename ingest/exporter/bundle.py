import re

from ingest.api import utils
from ingest.api.dssapi import DssApi
from ingest.exporter.metadata import MetadataResource

_BUNDLE_FILE_TYPE_DATA = 'data'
_BUNDLE_FILE_TYPE_LINKS = 'links'

_metadata_type_attr_map = {
    'biomaterial': 'fileBiomaterialMap',
    'file': 'fileFilesMap',
    'process': 'fileProcessMap',
    'project': 'fileProjectMap',
    'protocol': 'fileProtocolMap',
}


class BundleManifest:

    def __init__(self, bundleUuid=None, envelopeUuid=None, bundleVersion=None):
        self.bundleUuid = bundleUuid
        self.envelopeUuid = envelopeUuid if envelopeUuid is not None else {}
        self.bundleVersion = bundleVersion
        self.dataFiles = []
        self.fileBiomaterialMap = {}
        self.fileProcessMap = {}
        self.fileFilesMap = {}
        self.fileProjectMap = {}
        self.fileProtocolMap = {}

    def add_bundle_file(self, metadata_type, entry: dict):
        if metadata_type == _BUNDLE_FILE_TYPE_LINKS:
            pass  # we don't want to track links.json in BundleManifests
        elif metadata_type == _BUNDLE_FILE_TYPE_DATA:
            self.dataFiles.extend(entry.keys())
        else:
            attr_mapping = _metadata_type_attr_map.get(metadata_type)
            if attr_mapping:
                file_map = getattr(self, attr_mapping)
                file_map.update(entry)
            else:
                raise KeyError(f'Cannot map unknown metadata type [{metadata_type}].')


_DSS_CONTENT_TYPE_PATTERN = re.compile('.*dcp-type="?(metadata/)?(?P<data_type>\\w+)"?.*')

_DSS_CONTENT_TYPE_TEMPLATE = 'application/json; dcp-type="metadata/{0}"'


class BundleParseFromSourceException(Exception):
    pass


class Bundle:

    def __init__(self, uuid: str, files: dict, version: str, creator_uid: int):
        self.uuid = uuid
        self.files = files
        self.version = version
        self.creator_uid = creator_uid

    @staticmethod
    def bundle_from_source(source: dict):
        try:
            bundle_source = source['bundle']

            uuid = bundle_source['uuid']
            version = bundle_source['version']
            creator_uid = bundle_source['creator_uid']
            files = Bundle._prepare_file_map(bundle_source['files'])
            return Bundle(uuid, files, version, creator_uid)
        except (KeyError, TypeError) as e:
            raise BundleParseFromSourceException(e)

    @staticmethod
    def _prepare_file_map(bundle_files: list):
        return {file['uuid']: file for file in bundle_files}

    def get_version(self):
        return self.version

    def get_file(self, uuid):
        return self.files.get(uuid)

    def get_files(self):
        return list(self.files.values())

    def count_files(self):
        return len(self.files)

    def update_version(self, version):
        self.version = version

    def update_file(self, metadata_resource: MetadataResource):
        target_file = self.get_file(metadata_resource.uuid)
        target_file['version'] = utils.to_dss_version(metadata_resource.dcp_version)
        target_file['content-type'] = _DSS_CONTENT_TYPE_TEMPLATE.format(
            metadata_resource.metadata_type)

    def generate_manifest(self, envelope_uuid) -> BundleManifest:
        manifest = BundleManifest(bundleUuid=self.uuid, envelopeUuid=envelope_uuid,
                                  bundleVersion=self.get_version())
        for file_uuid, file in self.files.items():
            pattern_match = _DSS_CONTENT_TYPE_PATTERN.match(file.get('content-type'))
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
        return Bundle.bundle_from_source(bundle_source)

    def update(self, bundle: Bundle, staging_details: list):
        cloud_url_map = {info.metadata_uuid: info.cloud_url for info in staging_details}
        bundle_files = bundle.get_files()
        for uuid, cloud_url in cloud_url_map.items():
            file = bundle.get_file(uuid)
            self.dss_client.put_file(None, {'url': cloud_url, 'dss_uuid': uuid,
                                            'update_date': file.get('version')})
        self.dss_client.put_bundle(bundle.uuid, bundle.get_version(), bundle_files)
