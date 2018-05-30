import json


class IngestSubmitter(object):

    def __init__(self, ingest_api):
        # TODO the IngestSubmitter should probably build its own instance of IngestApi
        self.ingest_api = ingest_api

    def submit(self, entities_dictionaries, submission_url):

        submission = Submission(self.ingest_api, submission_url)

        for entity in entities_dictionaries.get_entities():
            submission.add_entity(entity)

        for entity in entities_dictionaries.get_entities():
            for link in entity.direct_links:
                to_entity = entities_dictionaries.get_entity(link['entity'], link['id'])
                try:
                    submission.link_entity(entity, to_entity, relationship=link['relationship'])
                except Exception as link_error:
                    # TODO use logging
                    print(f'The {entity.type} with {entity.id} could not be linked to {to_entity.type} with id {to_entity.id}')
                    print(f'{str(link_error)}')

        return submission


class EntityLinker(object):

    def __init__(self, template_manager):
        self.template_manager = template_manager
        self.process_id_ctr = 0

    def process_links(self, entities_dictionaries):
        for from_entity_type in entities_dictionaries.get_entity_types():
            entities_dict = entities_dictionaries.get_entities_of_type(from_entity_type)
            for from_entity_id, from_entity in entities_dict.items():
                self._validate_entity_links(entities_dictionaries, from_entity)
                self.generate_direct_links(entities_dictionaries, from_entity)

        return entities_dictionaries

    def generate_direct_links(self, entities_dictionaries, from_entity):
        project = entities_dictionaries.get_project()

        # link all entities to project
        if not from_entity.type == 'project':
            # TODO protocols and files don't have links to project in ingest-core
            from_entity.direct_links.append({
                'entity': 'project',
                'id': project.id,
                'relationship': 'projects'
            })

        links_by_entity = from_entity.links_by_entity

        linked_biomaterial_ids = links_by_entity.get('biomaterial') if links_by_entity.get('biomaterial') else []
        linked_process_id = links_by_entity['process'][0] if links_by_entity.get('process') else None
        linked_protocol_ids = links_by_entity.get('protocol') if links_by_entity.get('protocol') else []
        linked_file_ids = links_by_entity.get('file') if links_by_entity.get('file') else []

        if linked_biomaterial_ids or linked_file_ids:

            linking_process = self.link_process(entities_dictionaries, linked_process_id)
            linking_process.direct_links.append({
                'entity': 'project',
                'id': project.id,
                'relationship': 'projects'
            })
            entities_dictionaries.add_entity(linking_process)

            # link output of process
            from_entity.direct_links.append({
                'entity': linking_process.type,
                'id': linking_process.id,
                'relationship': 'derivedByProcesses'
            })

            # apply all protocols to the linking process
            for linked_protocol_id in linked_protocol_ids:
                linking_process.direct_links.append({
                    'entity': 'protocol',
                    'id': linked_protocol_id,
                    'relationship': 'protocols'
                })

            # biomaterial-biomaterial
            # file-biomaterial
            for linked_biomaterial_id in linked_biomaterial_ids:
                linked_biomaterial_entity = entities_dictionaries.get_entity('biomaterial', linked_biomaterial_id)
                linked_biomaterial_entity.direct_links.append({
                    'entity': linking_process.type,
                    'id': linking_process.id,
                    'relationship': 'inputToProcesses'
                })

            # file-file
            for linked_file_id in linked_file_ids:
                linked_file_entity = entities_dictionaries.get_entity('file', linked_file_id)
                linked_file_entity.direct_links.append({
                    'entity': linking_process.type,
                    'id': linking_process.id,
                    'relationship': 'inputToProcesses'
                })

    def link_process(self, entities_dictionaries, linked_process_id):
        linking_process = None

        if linked_process_id:
            linking_process = self.create_or_get_process(entities_dictionaries, linked_process_id)
        else:
            empty_process_id = self._generate_empty_process_id()
            linking_process = self._create_empty_process(empty_process_id)

        return linking_process

    def _validate_entity_links(self, entities_dictionaries, entity):
        links_by_entity = entity.links_by_entity

        for link_entity_type, link_entity_ids in links_by_entity.items():
            for link_entity_id in link_entity_ids:
                if not link_entity_type == 'process':  # it is expected that no processes are defined in any tab, these will be created later
                    if not self._is_valid_spreadsheet_link(entity.type, link_entity_type):
                        raise InvalidLinkInSpreadsheet(entity, link_entity_type, link_entity_id)
                    if not entities_dictionaries.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)
                    if not entities_dictionaries.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)


                if link_entity_type == 'process' and not len(link_entity_ids) == 1:
                    raise MultipleProcessesFound(entity, link_entity_ids)

    def create_or_get_process(self, entities_dictionaries, process_id):
        process = entities_dictionaries.get_entity('process', process_id)

        if not process:
            process = self._create_empty_process(process_id)

        return process

    @staticmethod
    def _is_valid_spreadsheet_link(from_entity_type, to_entity_type):
        VALID_ENTITY_LINKS_MAP = [
            'biomaterial-biomaterial',
            'file-biomaterial',
            'file-file',
            'biomaterial-process',
            'biomaterial-protocol',
            'file-process',
            'file-protocol',
        ]
        link_key = from_entity_type + '-' + to_entity_type

        return link_key in VALID_ENTITY_LINKS_MAP

    def _create_empty_process(self, empty_process_id):
        process_core = {"process_id": "process_" + str(empty_process_id)}
        schema_type = 'process'
        described_by = self.template_manager.get_schema_url(schema_type)

        obj = {"process_core": process_core, "schema_type": schema_type, "describedBy": described_by}

        process = Entity(
            type='process',
            id=empty_process_id,
            content=obj
        )
        return process

    def _generate_empty_process_id(self):
        self.process_id_ctr += 1

        return 'empty_process_id_' + str(self.process_id_ctr)


class Entity(object):
    def __init__(self, type, id, content, links_by_entity=None, direct_links=None):
        self.type = type
        self.id = id
        self.content = content
        self.links_by_entity = {} if links_by_entity is None else links_by_entity
        self.direct_links = [] if direct_links is None else direct_links

        self.ingest_json = None


class Submission(object):
    ENTITY_LINK = {
        'biomaterial': 'biomaterials',
        'process': 'processes',
        'file': 'files',
        'protocol': 'protocols',
        'project': 'projects'
    }

    def __init__(self, ingest_api, submission_url):
        self.ingest_api = ingest_api
        self.submission_url = submission_url
        self.metadata_dict = {}

    def get_submission_url(self):
        return self.submission_url

    def add_entity(self, entity: Entity):
        link_name = self.ENTITY_LINK[entity.type]

        # TODO: how to get filename?!!!
        if entity.type == 'file':
            file_name = entity.content['file_core']['file_name']
            response = self.ingest_api.createFile(self.submission_url, file_name, json.dumps(entity.content))
        elif entity.type == 'project':
            response = self.ingest_api.createProject(self.submission_url, json.dumps(entity.content))
        else:
            response = self.ingest_api.createEntity(self.submission_url, json.dumps(entity.content), link_name)

        entity.ingest_json = response
        self.metadata_dict[entity.type + '.' + entity.id] = entity

        return entity

    def get_entity(self, entity_type, id):
        key = entity_type + '.' + id
        return self.metadata_dict[key]

    def link_entity(self, from_entity, to_entity, relationship):
        from_entity_ingest = from_entity.ingest_json
        to_entity_ingest = to_entity.ingest_json
        self.ingest_api.linkEntity(from_entity_ingest, to_entity_ingest , relationship)


class EntitiesDictionaries(object):

    def __init__(self):
        self.entities_dict_by_type = {}

    @staticmethod
    def load(entity_json):
        dictionary = EntitiesDictionaries()
        for entity_type, entities_dict in entity_json.items():
            for entity_id, entity_body in entities_dict.items():
                entity = Entity(type=entity_type, id=entity_id, content=entity_body['content'],
                                links_by_entity=entity_body.get('links_by_entity', {}))
                dictionary.add_entity(entity)
        return dictionary

    def get_entity_types(self):
        return list(self.entities_dict_by_type.keys())

    def get_entities_of_type(self, type):
        entities_dict = self.entities_dict_by_type.get(type, {})

        return entities_dict

    def get_entity(self, type, id):
        if self.entities_dict_by_type.get(type) and self.entities_dict_by_type[type].get(id):
            return self.entities_dict_by_type[type][id]

    def add_entity(self, entity):
        entities_of_type = self.entities_dict_by_type.get(entity.type)
        if not entities_of_type:
            self.entities_dict_by_type[entity.type] = {}
            entities_of_type = self.entities_dict_by_type.get(entity.type)
        entities_of_type[entity.id] = entity

    def get_entities(self):
        all_entities = []
        for entity_type, entities_dict in self.entities_dict_by_type.items():
            all_entities.extend(entities_dict.values())

        return all_entities

    def get_project(self):
        project_id = list(self.entities_dict_by_type.get('project').keys())[0]
        return self.get_entity('project', project_id)


class InvalidEntityIngestLink(Exception):

    def __init__(self, from_entity, to_entity):
        message = f'It is not possible to link a {from_entity.type} to {to_entity.type} in ingest database.'
        super(InvalidEntityIngestLink, self).__init__(message)
        self.from_entity = from_entity
        self.to_entity = to_entity


class InvalidLinkInSpreadsheet(Exception):

    def __init__(self, from_entity, link_entity_type, link_entity_id):
        message = f'It is not possible to link a {from_entity.type} to {link_entity_type} in the spreadsheet.'
        super(InvalidLinkInSpreadsheet, self).__init__(message)
        self.from_entity = from_entity
        self.link_entity_type = link_entity_type
        self.link_entity_id = link_entity_id


class LinkedEntityNotFound(Exception):
    def __init__(self, from_entity, entity_type, id):
        message = f'A link from a {from_entity.type} with id {from_entity.id} to a {entity_type} with id, ' \
                  f'"{id}", is not found in the spreadsheet.'
        super(LinkedEntityNotFound, self).__init__(message)
        self.entity = entity_type
        self.id = id


class MultipleProcessesFound(Exception):
    def __init__(self, from_entity, process_ids):
        message = f'Multiple processes are linked {from_entity.type} in the spreadsheet: {process_ids}.'
        super(MultipleProcessesFound, self).__init__(message)

        self.process_ids = process_ids
        self.from_entity = from_entity

