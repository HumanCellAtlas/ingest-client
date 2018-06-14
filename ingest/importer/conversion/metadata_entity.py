import copy

from ingest.importer.data_node import DataNode


class MetadataEntity:

    def __init__(self, object_id=None, content={}, links={}, external_links={}):
        self.object_id = object_id
        self._content = DataNode(defaults=copy.deepcopy(content))
        self._links = copy.deepcopy(links)
        self._external_links = copy.deepcopy(external_links)

    @property
    def content(self):
        return copy.deepcopy(self._content)

    def get_content(self, content_property):
        return self._content[content_property]

    def define_content(self, content_property, value):
        self._content[content_property] = value

    @property
    def links(self):
        return copy.deepcopy(self._links)

    def get_links(self, link_entity_type):
        return self._links.get(link_entity_type)

    def add_links(self, link_entity_type, new_links):
        self._do_add_links(self._links, link_entity_type, new_links)

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
