class Entity(object):
    def __init__(self, type, id, content, links=[]):
        self.type = type
        self.id = id
        self.content = content
        self.links = links

        self.ingest_json = None


class Submission(object):
    ENTITY_LINK = {
        'biomaterial': 'biomaterials',
        'process': 'processes',
        'file': 'files',
        'protocol': 'protocols'
    }

    RELATION_LINK = {
        'biomaterial': {
            'process': 'inputToProcesses'
        },
        'process': {
            'protocol': 'protocols',
            'file': 'derivedByProcesses',
            'biomaterial': 'derivedByProcesses'
        },
        'file': {
            'process': 'inputToProcesses'
        },
    }

    def __init__(self, ingest_api):
        self.ingest_api = ingest_api
        self.submission_url = self.ingest_api.createSubmission()
        self.metadata_dict = {}

    def get_submission_url(self):
        return self.submission_url

    def add_entity(self, entity: Entity):
        link_name = self.ENTITY_LINK[entity.type]
        response = self.ingest_api.createEntity(self.submission_url, entity.content, link_name)
        entity.ingest_json = response
        self.metadata_dict[entity.type + '.' + entity.id] = entity

        return entity

    def get_entity(self, entity_type, id):
        key = entity_type + '.' + id
        return self.metadata_dict[key]

    def link_entity(self, from_entity, to_entity):
        if from_entity.type in self.RELATION_LINK and to_entity.type in self.RELATION_LINK[from_entity.type]:
            relationship = self.RELATION_LINK[from_entity.type][to_entity.type]
        else:
            raise InvalidEntityLink(from_entity, to_entity)

        self.ingest_api.linkEntity(from_entity.ingest_json, to_entity.ingest_json, relationship)


class IngestSubmitter(object):

    def __init__(self, ingest_api):
        self.ingest_api = ingest_api
        self.entities_by_type = {}

    def _generate_entities_map(self, spreadsheet_json):
        for entity_type, entities_dict in spreadsheet_json.items():
            for entity_id, entity_dict in entities_dict.items():
                entity = Entity(
                    type=entity_type,
                    id=entity_id,
                    content=entity_dict['content'],
                    links=entity_dict.get('links') if entity_dict.get('links') else []
                )

                if not self.entities_by_type.get(entity_type):
                    self.entities_by_type[entity_type] = {}

                if not self.entities_by_type[entity_type].get(entity_id):
                    self.entities_by_type[entity_type][entity_id] = {}

                self.entities_by_type[entity_type][entity_id] = entity
        return self.entities_by_type

    def submit(self, spreadsheet_json):
        entities_map = self._generate_entities_map(spreadsheet_json)

        submission = Submission(self.ingest_api)

        for entity_type, entities_dict in entities_map.items():
            for entity_id, entity in entities_dict.items():
                submission.add_entity(entity)

        return submission


class InvalidEntityLink(Exception):
    def __init__(self, from_entity, to_entity):
        self.from_entity = from_entity
        self.to_entity = to_entity
