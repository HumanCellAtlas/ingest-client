import json
import logging

format = '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=format)

class IngestSubmitter(object):

    def __init__(self, ingest_api):
        # TODO the IngestSubmitter should probably build its own instance of IngestApi
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)

    def submit(self, entity_map, submission_url):
        submission = Submission(self.ingest_api, submission_url)
        submission.define_manifest(entity_map)

        entities = entity_map.get_entities()

        self._add_entities(entities, submission)

        self._link_submission_to_project(entity_map, submission, submission_url)

        self._link_entities(entities, entity_map, submission)

        return submission

    def _link_submission_to_project(self, entity_map, submission, submission_url):
        project = entity_map.get_project()
        submission_envelope = self.ingest_api.getSubmissionEnvelope(submission_url)
        submission_entity = Entity('submission_envelope',
                                   submission_url,
                                   None,
                                   is_reference=True,
                                   ingest_json=submission_envelope
                                   )
        submission.link_entity(project, submission_entity, 'submissionEnvelopes')

    def _link_entities(self, entities, entity_map, submission):
        for entity in entities:
            for link in entity.direct_links:
                to_entity = entity_map.get_entity(link['entity'], link['id'])
                try:
                    submission.link_entity(entity, to_entity, relationship=link['relationship'])
                except Exception as link_error:
                    error_message = f'''The {entity.type} with id {entity.id} could not be 
                    linked to {to_entity.type} with id {to_entity.id}.'''
                    self.logger.error(error_message)
                    self.logger.error(f'{str(link_error)}')

    def _add_entities(self, entities, submission):
        for entity in entities:
            if not entity.is_reference:
                try:
                    submission.add_entity(entity)
                except:
                    error_message = f'error in entity [{entity.type}]:\n{entity.content}'
                    self.logger.error(error_message)
                    raise


class EntityLinker(object):

    def __init__(self, template_manager):
        self.template_manager = template_manager
        self.process_id_ctr = 0

    def process_links_from_spreadsheet(self, entity_map):
        for entity in entity_map.get_entities():
            self._validate_entity_links(entity_map, entity)
            self._generate_direct_links(entity_map, entity)

        return entity_map

    def _generate_direct_links(self, entity_map, entity):
        project = entity_map.get_project()

        # TODO Revisit if we need to link all entities to the project
        # currently, all entities are indirectly link to the project via the submission envelope
        # another issue is that protocols and files don't have links to project in ingest-core

        if project and not entity.type == 'project':
            if entity.type != 'protocol' and entity.type != 'file':
                entity.direct_links.append({
                    'entity': 'project',
                    'id': project.id,
                    'relationship': 'projects'
                })

        if project and entity.concrete_type == 'supplementary_file':
            project.direct_links.append({
                'entity': 'file',
                'id': entity.id,
                'relationship': 'supplementaryFiles'
            })

        links_by_entity = entity.links_by_entity

        linked_biomaterial_ids = links_by_entity.get('biomaterial', [])
        linked_process_id = links_by_entity['process'][0] if links_by_entity.get('process') else None
        linked_protocol_ids = links_by_entity.get('protocol', [])
        linked_file_ids = links_by_entity.get('file', [])

        linking_details = entity.linking_details

        if linked_biomaterial_ids or linked_file_ids:

            linking_process = self.link_process(entity_map, linked_process_id, linking_details)
            linking_process.direct_links.append({
                'entity': 'project',
                'id': project.id,
                'relationship': 'projects'
            })
            entity_map.add_entity(linking_process)

            # link output of process
            entity.direct_links.append({
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
                linked_biomaterial_entity = entity_map.get_entity('biomaterial', linked_biomaterial_id)
                linked_biomaterial_entity.direct_links.append({
                    'entity': linking_process.type,
                    'id': linking_process.id,
                    'relationship': 'inputToProcesses'
                })

            # file-file
            for linked_file_id in linked_file_ids:
                linked_file_entity = entity_map.get_entity('file', linked_file_id)
                linked_file_entity.direct_links.append({
                    'entity': linking_process.type,
                    'id': linking_process.id,
                    'relationship': 'inputToProcesses'
                })

    def link_process(self, entity_map, linked_process_id, linking_details):
        if not linked_process_id:
            linked_process_id = self._generate_empty_process_id()

        linking_process = self.create_or_get_process(entity_map, linked_process_id, linking_details)

        return linking_process

    def _validate_entity_links(self, entity_map, entity):
        links_by_entity = entity.links_by_entity

        for link_entity_type, link_entity_ids in links_by_entity.items():
            for link_entity_id in link_entity_ids:
                if not link_entity_type == 'process':  # it is expected that no processes are defined in any tab, these will be created later
                    if not self._is_valid_spreadsheet_link(entity.type, link_entity_type):
                        raise InvalidLinkInSpreadsheet(entity, link_entity_type, link_entity_id)
                    if not entity_map.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)
                    if not entity_map.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)

                if link_entity_type == 'process' and not len(link_entity_ids) == 1:
                    raise MultipleProcessesFound(entity, link_entity_ids)

    def create_or_get_process(self, entity_map, process_id, linking_details):
        process = entity_map.get_entity('process', process_id)

        if not process:
            process = self.create_process(process_id, linking_details)

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

    def create_process(self, process_id, linking_details):
        schema_type = 'process'
        described_by = self.template_manager.get_schema_url(schema_type)

        if linking_details:
            if not linking_details.get('process_core'):
                linking_details['process_core'] = {}

            linking_details['process_core']['process_id'] = process_id
            linking_details['schema_type'] = schema_type
            linking_details['describedBy'] = described_by
        else:
            process_core = {'process_id': process_id}
            linking_details = {
                "process_core": process_core,
                "schema_type": schema_type,
                "describedBy": described_by
            }

        process = Entity(
            entity_type='process',
            entity_id=process_id,
            content=linking_details
        )

        return process

    def _generate_empty_process_id(self):
        self.process_id_ctr += 1

        return 'process_id_' + str(self.process_id_ctr)


class Entity(object):

    def __init__(self, entity_type, entity_id, content, ingest_json=None, links_by_entity=None,
                 direct_links=None, is_reference=False, linking_details=None, concrete_type=None):
        self.type = entity_type
        self.id = entity_id
        self.content = content
        self._prepare_links_by_entity(links_by_entity)
        self._prepare_direct_links(direct_links)
        self._prepare_linking_details(linking_details)
        self.ingest_json = ingest_json
        self.is_reference = is_reference
        self.concrete_type = concrete_type

    def _prepare_links_by_entity(self, links_by_entity):
        self.links_by_entity = {}
        if links_by_entity is not None:
            self.links_by_entity.update(links_by_entity)

    def _prepare_direct_links(self, direct_links):
        self.direct_links = []
        if direct_links is not None:
            self.direct_links.extend(direct_links)

    def _prepare_linking_details(self, linking_details):
        self.linking_details = {}
        if linking_details is not None:
            self.linking_details.update(linking_details)


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
        if from_entity.is_reference and not from_entity.ingest_json:
            from_entity.ingest_json = self.ingest_api.getEntityByUuid(self.ENTITY_LINK[from_entity.type], from_entity.id)

        if to_entity.is_reference and not to_entity.ingest_json:
            to_entity.ingest_json = self.ingest_api.getEntityByUuid(self.ENTITY_LINK[to_entity.type], to_entity.id)

        from_entity_ingest = from_entity.ingest_json
        to_entity_ingest = to_entity.ingest_json
        self.ingest_api.linkEntity(from_entity_ingest, to_entity_ingest, relationship)

    def define_manifest(self, entity_map):
        total_count = entity_map.count_total()

        # TODO provide a better way to serialize
        manifest_json = json.dumps({
            'totalCount': entity_map.count_total(),
            'expectedBiomaterials': entity_map.count_entities_of_type('biomaterial'),
            'expectedProcesses': entity_map.count_entities_of_type('process'),
            'expectedFiles': entity_map.count_entities_of_type('file'),
            'expectedProtocols': entity_map.count_entities_of_type('protocol'),
            'expectedProjects': entity_map.count_entities_of_type('project')
        })

        self.ingest_api.createSubmissionManifest(self.submission_url, manifest_json)


class EntityMap(object):

    def __init__(self, *entities):
        self.entities_dict_by_type = {}
        if entities is not None:
            for entity in entities:
                self.add_entity(entity)

    @staticmethod
    def load(entity_json):
        dictionary = EntityMap()

        for entity_type, entities_dict in entity_json.items():
            for entity_id, entity_body in entities_dict.items():

                external_links = entity_body.get('external_links_by_entity')

                if not external_links:
                    external_links = {}

                for external_link_type, external_link_uuids in external_links.items():
                    for entity_uuid in external_link_uuids:
                        external_link_entity = Entity(entity_type=external_link_type,
                                                      entity_id=entity_uuid,
                                                      content=None,
                                                      is_reference=True)

                        dictionary.add_entity(external_link_entity)

                        if not entity_body.get('links_by_entity'):
                            entity_body['links_by_entity'] = {}

                        if not entity_body['links_by_entity'].get(external_link_type):
                            entity_body['links_by_entity'][external_link_type] = []

                        entity_body['links_by_entity'][external_link_type].append(entity_uuid)

                entity = Entity(entity_type=entity_type,
                                entity_id=entity_id,
                                content=entity_body.get('content'),
                                links_by_entity=entity_body.get('links_by_entity', {}),
                                is_reference=entity_body.get('is_reference', False),
                                linking_details=entity_body.get('linking_details', {}),
                                concrete_type=entity_body.get('concrete_type'))

                dictionary.add_entity(entity)

        return dictionary

    def get_entity_types(self):
        return list(self.entities_dict_by_type.keys())

    def get_entities_of_type(self, type):
        entities = []

        entities_dict = self.entities_dict_by_type.get(type, {})

        for entity_id, entity in entities_dict.items():
            entities.append(entity)

        return entities

    def get_new_entities_of_type(self, type):
        entities = []
        entities_dict = self.entities_dict_by_type.get(type, {})
        for entity_id, entity in entities_dict.items():
            if not entity.is_reference:
                entities.append(entity)

        return entities

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

    def get_new_entities(self):
        all_entities = []
        for entity_type, entities_dict in self.entities_dict_by_type.items():
            for entity_id, entity in entities_dict.items():
                if not entity.is_reference:
                    all_entities.append(entity)
        return all_entities

    def get_project(self):
        project_id = list(self.entities_dict_by_type.get('project').keys())[0]
        return self.get_entity('project', project_id)

    def count_total(self):
        return len(self.get_entities())

    def count_entities_of_type(self, type):
        return len(self.get_new_entities_of_type(type))


class Error(Exception):
    def __init__(self, code, message):
        super(Error, self).__init__(message)
        self.code = code
        self.message = message


class InvalidEntityIngestLink(Error):
    def __init__(self, from_entity, to_entity):
        message = f'It is not possible to link a {from_entity.type} to {to_entity.type} in ingest database.'
        super(InvalidEntityIngestLink, self).__init__('InvalidEntityIngestLink', message)
        self.from_entity = from_entity
        self.to_entity = to_entity


class InvalidLinkInSpreadsheet(Error):
    def __init__(self, from_entity, link_entity_type, link_entity_id):
        message = f'It is not possible to link a {from_entity.type} to {link_entity_type} in the spreadsheet.'
        super(InvalidLinkInSpreadsheet, self).__init__('InvalidLinkInSpreadsheet', message)
        self.from_entity = from_entity
        self.link_entity_type = link_entity_type
        self.link_entity_id = link_entity_id


class LinkedEntityNotFound(Error):
    def __init__(self, from_entity, entity_type, id):
        message = f'A link from a {from_entity.type} with id {from_entity.id} to a {entity_type} with id, ' \
                  f'"{id}", is not found in the spreadsheet.'
        super(LinkedEntityNotFound, self).__init__('LinkedEntityNotFound', message)
        self.entity = entity_type
        self.id = id


class MultipleProcessesFound(Error):
    def __init__(self, from_entity, process_ids):
        message = f'Multiple processes are linked {from_entity.type} in the spreadsheet: {process_ids}.'
        super(MultipleProcessesFound, self).__init__('MultipleProcessesFound', message)

        self.process_ids = process_ids
        self.from_entity = from_entity

