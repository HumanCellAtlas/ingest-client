from ingest.importer.data_node import DataNode


class MetadataEntity:

    def __init__(self):
        self.content = DataNode()
        self.links = {}
        self.external_links = {}

    def get_content(self, content_property):
        return self.content[content_property]

    def define_content(self, content_property, value):
        self.content[content_property] = value

    def get_links(self, link_entity_type):
        return self.links.get(link_entity_type)

    def add_links(self, link_entity_type, new_links):
        self._do_add_links(self.links, link_entity_type, new_links)

    def get_external_links(self, link_entity_type):
        return self.external_links.get(link_entity_type)

    def add_external_links(self, link_entity_type, new_links):
        self._do_add_links(self.external_links, link_entity_type, new_links)

    @staticmethod
    def _do_add_links(link_map, link_entity_type, new_links):
        existent_links = link_map.get(link_entity_type)
        if existent_links is None:
            existent_links = []
            link_map[link_entity_type] = existent_links
        existent_links.extend(new_links)
