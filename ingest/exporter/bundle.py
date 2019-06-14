from copy import deepcopy

from ingest.api import utils
from ingest.api.dssapi import DssApi
from ingest.exporter.metadata import MetadataResource


class Bundle:

    def __init__(self, source={}):
        self._source = deepcopy(source)
        self._bundle = self._source.get('bundle')  # because bundle is nested in the root ¯\_(ツ)_/¯
        self._prepare_file_map()
        self.uuid = self._bundle.get('uuid')

    def _prepare_file_map(self):
        bundle_files = self._bundle.get('files') if self._bundle else None
        if not bundle_files:
            bundle_files = []
        self._file_map = {file.get('uuid'): file for file in bundle_files}

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
        target_file['content-type'] = f'metadata/{metadata_resource.metadata_type}'


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
