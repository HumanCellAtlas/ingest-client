import copy

from ingest.importer.data_node import DataNode
from ingest.importer.spreadsheet.ingest_worksheet import IngestRow

TYPE_UNDEFINED = 'undefined'


class MetadataEntity:

    # TODO enforce definition of concrete and domain types for all MetadataEntity
    # It's only currently done this way to minimise friction with other parts of the system
    def __init__(self, concrete_type=TYPE_UNDEFINED, domain_type=TYPE_UNDEFINED, object_id=None,
                 content={}, links={}, external_links={}, linking_details={}, row: IngestRow = None,
                 is_reference=False):
        self._concrete_type = concrete_type
        self._domain_type = domain_type
        self.object_id = object_id
        self._content = DataNode(defaults=copy.deepcopy(content))
        self._links = copy.deepcopy(links)
        self._external_links = copy.deepcopy(external_links)
        self._linking_details = DataNode(defaults=copy.deepcopy(linking_details))
        self._spreadsheet_location = {
            'row_index': row.index,
            'worksheet_title': row.worksheet_title,
        } if row else None
        self._is_reference = is_reference

    @property
    def concrete_type(self):
        return self._concrete_type

    @property
    def domain_type(self):
        return self._domain_type

    @property
    def content(self):
        return copy.deepcopy(self._content)

    def get_content(self, content_property):
        return self._content[content_property]

    def define_content(self, content_property, value):
        self._content[content_property] = value

    def define_linking_detail(self, link_property, value):
        self._linking_details[link_property] = value

    @property
    def linking_details(self):
        return self._linking_details.as_dict()

    def get_linking_detail(self, link_property):
        return self._linking_details[link_property]

    @property
    def links(self):
        return copy.deepcopy(self._links)

    def get_links(self, link_entity_type):
        return self._links.get(link_entity_type)

    def add_links(self, link_entity_type, new_links):
        self._do_add_links(self._links, link_entity_type, new_links)

    @property
    def is_reference(self):
        return self._is_reference

    @property
    def external_links(self):
        return copy.deepcopy(self._external_links)

    def get_external_links(self, link_entity_type):
        return self._external_links.get(link_entity_type)

    def add_external_links(self, link_entity_type, new_links):
        self._do_add_links(self._external_links, link_entity_type, new_links)

    @staticmethod
    def _do_add_links(link_map, link_entity_type, new_links):
        existent_links = link_map.get(link_entity_type)
        if existent_links is None:
            existent_links = []
            link_map[link_entity_type] = existent_links
        existent_links.extend(new_links)

    def retain_content_fields(self, *fields):
        for key in self._content.keys():
            if key not in fields:
                self._content.remove_field(key)

    def add_module_entity(self, module_entity):
        for field, value in module_entity.content.as_dict().items():
            module_list = self._content[field]
            if not module_list:
                module_list = []
                self._content[field] = module_list
            module_list.append(value)

    def map_for_submission(self):
        return {
            'is_reference': self.is_reference,
            'concrete_type': self.concrete_type,
            'content': self._content.as_dict(),
            'links_by_entity': self.links,
            'external_links_by_entity': self.external_links,
            'linking_details': self.linking_details,
            'spreadsheet_location': self._spreadsheet_location
        }
