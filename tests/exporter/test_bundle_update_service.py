from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.bundle_update_service import BundleUpdateService, MetadataResource, \
    StagingService


class MetadataResourceTest(TestCase):

    def test_from_dict(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = {'entityType': 'donor_organism',
                'uuid': {'uuid': uuid_value},
                'content': {'description': 'test'},
                'dcpVersion': '6.9.1'}

        # when:
        metadata = MetadataResource.from_dict(data)

        # then:
        self.assertIsNotNone(metadata)
        self.assertEqual(data['entityType'], metadata.metadata_type)
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
        self.assertEqual('9b159cae-a1fe-4cce-94bc-146e4aa20553.5.1.0.json',
                         metadata_resource_1.get_staging_file_name())
        self.assertEqual('38e0ee7c-90dc-438a-a0ed-071f9231f590.1.0.7.json',
                         metadata_resource_2.get_staging_file_name())


class StagingServiceTest(TestCase):

    def test_stage_update(self):
        # given:
        metadata_resource = MetadataResource(metadata_type='donor_organism',
                                             uuid='831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370',
                                             metadata_json={'description': 'test'},
                                             dcp_version='4.2.1')

        # and:
        staging_service = StagingService()

        # when:
        file_description = staging_service.stage_update(metadata_resource)

        # then:
        self.assertIsNotNone(file_description)


class BundleUpdateServiceTest(TestCase):

    def test_update_bundle(self):
        # given:
        staging_client = Mock(name='staging_client')
        dss_client = Mock(name='dss_client')
        ingest_client = Mock(name='ingest_client')
        service = BundleUpdateService(staging_client, dss_client, ingest_client)

        # and:
        test_file_description = FileDescription([], 'file', 'sample.ss2', 1024, 'sample.url')
        staging_client.stageFileRequest = Mock(return_value=test_file_description)

        # and:
        updated_bundle = {'files': [{}]}
        dss_client.get_bundle = Mock(return_value=updated_bundle)
        dss_client.create_file = Mock(return_value={})
        dss_client.put_bundle = Mock(return_value={})

        # and:
        ingest_client.get_entity_by_callback_link = Mock(return_value={})

        # when:
        update_submission = {'stagingDetails': {'stagingAreaUuid': {'uuid': '3cce991'}}}
        callback_list = ['ingest-api/23cb771']
        updated_bundle = service.update_bundle(update_submission, '67c9d90', 'v3', callback_list)

        # expect:
        self.assertIsNotNone(updated_bundle)
