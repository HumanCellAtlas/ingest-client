from copy import deepcopy


class MetadataParseException(Exception):
    pass


class MetadataProvenance:
    def __init__(self, document_id: str, submission_date: str, update_date: str):
        self.document_id = document_id
        self.submission_date = submission_date
        self.update_date = update_date

    def to_dict(self):
        return deepcopy(self.__dict__)


class MetadataResource:

    def __init__(self, metadata_type, metadata_json, uuid, dcp_version, provenance: MetadataProvenance):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version
        self.metadata_type = metadata_type
        self.provenance = provenance

    @staticmethod
    def from_dict(data: dict):
        try:
            metadata_json = data['content']
            uuid = data['uuid']['uuid']
            dcp_version = data['dcpVersion']
            metadata_type = data['type'].lower()
            provenance = MetadataResource.provenance_from_dict(data)
            return MetadataResource(metadata_type, metadata_json, uuid, dcp_version, provenance)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    @staticmethod
    def provenance_from_dict(data: dict):
        try:
            uuid = data['uuid']['uuid']
            submission_date = data['submissionDate']
            update_date = data['updateDate']

            return MetadataProvenance(uuid, submission_date, update_date)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    def get_staging_file_name(self):
        return f'{self.metadata_type}_{self.uuid}.json'

    def to_bundle_metadata(self) -> dict:
        bundle_metadata = dict()

        content = deepcopy(self.metadata_json)
        provenance = {'provenance': self.provenance.to_dict()}

        bundle_metadata.update(content)
        bundle_metadata.update(provenance)
        return bundle_metadata


class MetadataService:

    def __init__(self, ingest_client):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)
