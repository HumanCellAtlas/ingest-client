class MetadataParseException(Exception):
    pass

class MetadataTypeParseException(Exception):
    pass

class MetadataResource:

    def __init__(self, metadata_type=None, metadata_json=None, uuid=None, dcp_version=None):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version
        self.metadata_type = metadata_type

    @staticmethod
    def _determine_metadata_type(metadata_url: str):
        resource_path_type_map = {
            "biomaterials": "biomaterial",
            "projects": "project",
            "files": "file",
            "protocols": "protocol",
            "processes": "process"
        }

        resource_path = metadata_url.split("/")[-2]
        if resource_path in resource_path_type_map:
            return resource_path_type_map[resource_path]
        else:
            raise MetadataTypeParseException(msg=f'Failed to parse metadata type for resource at {metadata_url}')

    @staticmethod
    def from_dict(data: dict):
        try:
            uuid = data['uuid']['uuid']
            content = data['content']
            dcp_version = data['dcpVersion']
            self_url = data['_links']['self']['href']
            metadata_type = MetadataResource._determine_metadata_type(self_url)
            metadata_resource = MetadataResource(metadata_type=metadata_type, uuid=uuid, metadata_json=content,
                                                 dcp_version=dcp_version)
            return metadata_resource
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
