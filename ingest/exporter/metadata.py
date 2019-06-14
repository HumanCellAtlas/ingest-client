class MetadataResource:

    def __init__(self, metadata_type=None, metadata_json=None, uuid=None, dcp_version=None):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version
        self.metadata_type = metadata_type
        if not metadata_type:
            self._determine_metadata_type()

    def _determine_metadata_type(self):
        metadata_type = None
        if self.metadata_json:
            metadata_type = self.metadata_json.get('schema_type')
        self.metadata_type = metadata_type

    @staticmethod
    def from_dict(data: dict):
        uuid_object = data.get('uuid')
        uuid = uuid_object.get('uuid') if uuid_object else None
        content = data.get('content')
        metadata_resource = MetadataResource(uuid=uuid, metadata_json=content,
                                             dcp_version=data.get('dcpVersion'))
        return metadata_resource

    def get_staging_file_name(self):
        return f'{self.metadata_type}_{self.uuid}.json'


class MetadataService:

    def __init__(self, ingest_client):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)
