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

    def test_add_links_multiple_times(self):
        # given:
        metadata = MetadataEntity()

        # when:
        batch_1 = ['78de112', '963fefed']
        batch_2 = ['2daab01']
        metadata.add_links('item', batch_1)
        metadata.add_links('item', batch_2)

        # then:
        item_links = metadata.get_links('item')
        self.assertIsNotNone(item_links)
        self.assertCountEqual(batch_1 + batch_2, item_links)

    def test_add_external_links(self):
        # given:
        metadata = MetadataEntity()

        # when:
        new_links = ['77701ee', '254aefb']
        metadata.add_external_links('file', new_links)

        # then:
        file_links = metadata.get_external_links('file')
        self.assertIsNotNone(file_links)
        self.assertCountEqual(new_links, file_links)