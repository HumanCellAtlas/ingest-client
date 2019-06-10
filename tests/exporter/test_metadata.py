from unittest import TestCase

from mock import Mock

from ingest.exporter.metadata import MetadataResource, MetadataService


class MetadataResourceTest(TestCase):

    def test_from_dict(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = {'entityType': 'biomaterial',
                'uuid': {'uuid': uuid_value},
                'content': {'describedBy': 'https://hca.tld/types/donor_organism',
                            'description': 'test'},
                'dcpVersion': '6.9.1'}

        # when:
        metadata = MetadataResource.from_dict(data)

        # then:
        self.assertIsNotNone(metadata)
        self.assertEqual('donor_organism', metadata.metadata_type)
        self.assertEqual(data['content'], metadata.metadata_json)
        self.assertEqual(data['dcpVersion'], metadata.dcp_version)

        # and:
        self.assertEqual(uuid_value, metadata.uuid)

    def test_from_dict_with_missing_info(self):
        # given:
        data = {'entityType': 'cell_suspension'}

        # when:
        metadata = MetadataResource.from_dict(data)

        # then:
        self.assertIsNone(metadata.dcp_version)
        self.assertIsNone(metadata.uuid)

    def test_get_staging_file_name(self):
        # given:
        metadata_resource_1 = MetadataResource(metadata_type='specimen',
                                               uuid='9b159cae-a1fe-4cce-94bc-146e4aa20553',
                                               metadata_json={'description': 'test'},
                                               dcp_version='5.1.0')
        metadata_resource_2 = MetadataResource(metadata_type='donor_organism',
                                               uuid='38e0ee7c-90dc-438a-a0ed-071f9231f590',
                                               metadata_json={'text': 'sample'},
                                               dcp_version='1.0.7')

        # expect:
        self.assertEqual('specimen_9b159cae-a1fe-4cce-94bc-146e4aa20553.json',
                         metadata_resource_1.get_staging_file_name())
        self.assertEqual('donor_organism_38e0ee7c-90dc-438a-a0ed-071f9231f590.json',
                         metadata_resource_2.get_staging_file_name())


class MetadataServiceTest(TestCase):

    def test_fetch_resource(self):
        # given:
        ingest_client = Mock(name='ingest_client')
        uuid = '301636f7-f97b-4379-bf77-c5dcd9f17bcb'
        raw_metadata = {'entityType': 'biomaterial',
                        'uuid': {'uuid': uuid},
                        'content': {'describedBy': 'https://hca.tld/types/cell_suspension',
                            'text': 'test'},
                        'dcpVersion': '8.2.7'}
        ingest_client.get_entity_by_callback_link = Mock(return_value=raw_metadata)

        # and:
        metadata_service = MetadataService(ingest_client)

        # when:
        metadata_resource = metadata_service.fetch_resource(
            'hca.domain.com/api/cellsuspensions/301636f7-f97b-4379-bf77-c5dcd9f17bcb')

        # then:
        self.assertEqual('cell_suspension', metadata_resource.metadata_type)
        self.assertEqual(uuid, metadata_resource.uuid)
        self.assertEqual(raw_metadata['content'], metadata_resource.metadata_json)
        self.assertEqual(raw_metadata['dcpVersion'], metadata_resource.dcp_version)
