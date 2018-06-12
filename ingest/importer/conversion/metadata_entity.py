class MetadataEntity:

    def __init__(self):
        self.content = {}
        self.links = {}
        self.external_links = {}

    def get_links(self, link_entity_type):
        return self.links.get(link_entity_type)

    def add_links(self, link_entity_type, new_links):
        existent_links = self.get_links(link_entity_type)
        if existent_links is None:
            existent_links = []
            self.links[link_entity_type] = existent_links
        existent_links.extend(new_links)

    def get_external_links(self, link_entity_type):
        return self.external_links.get(link_entity_type)

    def add_external_links(self, link_entity_type, new_links):
        self.external_links[link_entity_type] = []
