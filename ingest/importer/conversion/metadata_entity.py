class MetadataEntity:

    def __init__(self):
        self.content = {}
        self.links = {}
        self.external_links = {}

    def get_links(self, link_entity_type):
        return self.links.get(link_entity_type)

    def add_links(self, link_entity_type, links):
        self.links[link_entity_type] = links
