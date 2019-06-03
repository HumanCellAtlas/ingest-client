from unittest import TestCase

from mock import Mock

from ingest.exporter.bundle_update_service import BundleUpdateService


class TestBundleUpdateService(TestCase):

    def test_update_bundle(self):
        # given:
        dss_client = Mock(name='dss_client')
        service = BundleUpdateService(Mock(name='staging_client'), dss_client,
                                      Mock(name='ingest_client'))

        # and:
        updated_bundle = {'files': []}
        dss_client.get_bundle = Mock(return_value=updated_bundle)

        # when:
        update_submission = {'stagingDetails': {'stagingAreaUuid': {'uuid': '3cce991'}}}
        service.update_bundle(update_submission, '67c9d90', 'v3', [])

        # expect:
        self.assertIsNotNone(service)