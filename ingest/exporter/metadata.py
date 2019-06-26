class MetadataParseException(Exception):
    pass


class MetadataResource:

    def __init__(self, metadata_type, metadata_json, uuid, dcp_version):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version
        self.metadata_type = metadata_type

    @staticmethod
    def from_dict(data: dict):
        try:
            uuid = data['uuid']['uuid']
            content = data['content']
            dcp_version = data['dcpVersion']
            metadata_type = data['type']
            return MetadataResource(metadata_type, content, uuid, dcp_version)
        except KeyError as e:
            raise MetadataParseException(e)

    def get_staging_file_name(self):
        return f'{self.metadata_type}_{self.uuid}.json'


class MetadataService:

    def __init__(self, ingest_client):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)
