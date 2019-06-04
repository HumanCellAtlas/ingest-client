from unittest import TestCase

from mock import Mock

from ingest.exporter.bundle_update_service import BundleUpdateService


class TestBundleUpdateService(TestCase):

    def test_update_bundle(self):
        # given:
        dss_client = Mock(name='dss_client')
        ingest_client = Mock(name='ingest_client')
        service = BundleUpdateService(Mock(name='staging_client'), dss_client, ingest_client)

        # and:
        updated_bundle = {'files': ['x']}
        dss_client.get_bundle = Mock(return_value=updated_bundle)

        # and:
        ingest_client.get_entity_by_callback_link = Mock(return_value={})

        # when:
        update_submission = {'stagingDetails': {'stagingAreaUuid': {'uuid': '3cce991'}}}
        callback_list = ['ingest-api/23cb771']
        service.update_bundle(update_submission, '67c9d90', 'v3', callback_list)

        # expect:
        self.assertIsNotNone(service)