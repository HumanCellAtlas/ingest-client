from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.bundle_update_service import BundleUpdateService, MetadataResource


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
        self.assertEqual(data['entityType'], metadata.metdata_type)
        self.assertEqual(data['content'], metadata.metadata_json)
        self.assertEqual(data['dcpVersion'], metadata.dcp_version)

        # and:
        self.assertEqual(uuid_value, metadata.uuid)


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
