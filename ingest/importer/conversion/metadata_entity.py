class MetadataEntity:

    def __init__(self):
        self.content = {}
        self.links = {}
        self.external_links = {}

    def add_links(self, category, links):
        self.links[category] = []
