from unittest import TestCase

from ingest.importer.conversion.metadata_entity import MetadataEntity


class MetadataEntityTest(TestCase):

    def test_add_links(self):
        # given:
        metadata = MetadataEntity()

        # when:
        metadata.add_links('profile', ['73f909', '83fddf1', '9004811'])

        # then:
        profile_links = metadata.links.get('profile')
        self.assertIsNotNone(profile_links)
