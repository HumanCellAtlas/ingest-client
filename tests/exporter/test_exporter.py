from unittest import TestCase

from mock import Mock, call

from ingest.exporter.bundle import Bundle
from ingest.exporter.exporter import Exporter, SubmissionEnvelope
from ingest.exporter.metadata import MetadataResource
from ingest.exporter.staging import StagingInfo


def _create_test_bundle_file(uuid='', name='', content_type_prefix='metadata',
                             content_type='biomaterial', version='', indexed=True):
    dcp_type = f'"{content_type_prefix}/{content_type}"' if content_type_prefix else content_type
    return {'content-type': f'application/json; dcp-type={dcp_type}', 'uuid': uuid,
            'name': name, 'version': version, 'indexed': indexed}


class SubmissionEnvelopeTest(TestCase):

    def test_get_uuid(self):
        # given:
        no_uuid = SubmissionEnvelope()
        submission_uuid = 'e2bd764f-9f75-40a6-85fd-5bbeba6964ce'
        valid = SubmissionEnvelope(source={
            'uuid': {
                'uuid': submission_uuid
            }
        })
        incomplete = SubmissionEnvelope(source={'uuid': {}})

        # expect:
        self.assertIsNone(no_uuid.uuid)
        self.assertEqual(submission_uuid, valid.uuid)
        self.assertIsNone(incomplete.uuid)

    def test_get_staging_area_uuid(self):
        # given:
        no_staging_area = SubmissionEnvelope()
        staging_area_uuid = 'c3b4dee4-a6d0-41b0-b1b4-726d95995d4d'
        valid = SubmissionEnvelope(source={
            'stagingDetails': {
                'stagingAreaUuid': {
                    'uuid': staging_area_uuid
                }
            }
        })
        incomplete = SubmissionEnvelope(source={
            'stagingDetails': {}
        })

        # expect:
        self.assertIsNone(no_staging_area.staging_area_uuid)
        self.assertEqual(staging_area_uuid, valid.staging_area_uuid)
        self.assertIsNone(incomplete.staging_area_uuid)


class ExporterTest(TestCase):

    def test_export_update(self):
        # given:
        ingest_api = Mock(name='ingest_api')
        metadata_service = Mock(name='metadata_service')
        staging_service = Mock(name='staging_service')
        bundle_service = Mock(name='bundle_servie')
        exporter = Exporter(ingest_api, metadata_service, staging_service, bundle_service)

        # and:
        metadata_uuids = ['0c113d6c-4d3c-4e4a-8134-ae3050e663a6',
                          '37ee1172-8b2d-4500-a3fc-28b46921461b',
                          'e4e62047-69ff-45dc-a4f1-a5346249d217']
        bundle_uuid = '9dfca176-0ddf-4384-8b71-b74237edb8be'

        # and:
        metadata_resources = self._set_up_metadata_service(metadata_service, metadata_uuids)
        staging_details = self._set_up_staging_service(staging_service)
        bundle = self._set_up_bundle_service(bundle_service, bundle_uuid, metadata_uuids)

        # and:
        metadata_urls = [f'https://data.hca.tld/metadata/{uuid}' for uuid in metadata_uuids]
        staging_area_uuid = '947a6528-184e-4a05-9af5-355e1f450609'
        update_submission = {'uuid': {'uuid': 'subuuid'},
                             'stagingDetails': {'stagingAreaUuid': {'uuid': staging_area_uuid}}}
        update_version = '2019-06-09T1913000.000000Z'

        # when:
        exporter.export_update(update_submission, bundle_uuid, metadata_urls, update_version)

        # then:
        staging_service.stage_update.assert_has_calls(
            [call(staging_area_uuid, metadata) for metadata in metadata_resources], any_order=True)
        bundle.update_file.assert_has_calls([call(metadata) for metadata in metadata_resources],
                                            any_order=True)
        bundle.update_version.assert_called_with(update_version)
        bundle_service.update.assert_called_once_with(bundle, staging_details)

        # and: assert correct bundle manifest generated
        main_arg, *_ = ingest_api.create_bundle_manifest.call_args #unpack list of args
        bundle_manifest = main_arg[0]
        self.assertEqual(len(metadata_uuids), len(bundle_manifest.fileBiomaterialMap))

    @staticmethod
    def _set_up_metadata_service(metadata_service, metadata_uuids):
        metadata_version = '2019-06-12T16:12:20.087Z'
        metadata_resources = [MetadataResource("biomaterial", {}, uuid, metadata_version) for uuid in metadata_uuids]
        metadata_service.fetch_resource = Mock(side_effect=metadata_resources)
        return metadata_resources

    @staticmethod
    def _set_up_staging_service(staging_service):
        cloud_urls = [f'https://upload.tld/metadata{i}.json' for i in range(0, 3)]
        staging_details = [StagingInfo(cloud_url=url) for url in cloud_urls]
        staging_service.stage_update = Mock(side_effect=staging_details)
        return staging_details

    @staticmethod
    def _set_up_bundle_service(bundle_service, bundle_uuid, metadata_uuids):
        bundle_files = [{'uuid': uuid} for uuid in metadata_uuids]
        bundle = Bundle.bundle_from_source({
            'bundle': {
                'uuid': bundle_uuid,
                'version':'2019-06-12T16:12:20.087Z',
                'creator_uid': 5050,
                'files': bundle_files
            }
        })
        bundle = Mock(wraps=bundle)
        bundle_service.fetch = Mock(return_value=bundle)
        return bundle
