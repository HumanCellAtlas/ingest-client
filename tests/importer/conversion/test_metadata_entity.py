from unittest import TestCase

from ingest.importer.conversion.metadata_entity import MetadataEntity


class MetadataEntityTest(TestCase):

    def test_add_links(self):
        # given:
        metadata = MetadataEntity()

        # when:
        new_links = ['73f909', '83fddf1', '9004811']
        metadata.add_links('profile', new_links)

        # then:
        profile_links = metadata.get_links('profile')
        self.assertIsNotNone(profile_links)
        self.assertCountEqual(new_links, profile_links)
