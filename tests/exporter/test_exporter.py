from unittest import TestCase

from mock import Mock, call

from ingest.exporter.bundle import Bundle
from ingest.exporter.exporter import Exporter
from ingest.exporter.metadata import MetadataResource
from ingest.exporter.staging import StagingInfo


def _create_test_bundle_file(uuid='', name='', content_type='biomaterial', version='',
                             indexed=True):
    return {'content-type': f'application/json; dcp-type="{content_type}"', 'uuid': uuid,
            'name': name, 'version': version, 'indexed': indexed}


class ExporterTest(TestCase):

    def test_export_update(self):
        # given:
        metadata_service = Mock(name='metadata_service')
        staging_service = Mock(name='staging_service')
        bundle_service = Mock(name='bundle_servie')
        exporter = Exporter(metadata_service, staging_service, bundle_service)

        # and:
        metadata_uuids = ['0c113d6c-4d3c-4e4a-8134-ae3050e663a6',
                          '37ee1172-8b2d-4500-a3fc-28b46921461b',
                          'e4e62047-69ff-45dc-a4f1-a5346249d217']
        metadata_resources = [MetadataResource(uuid=uuid) for uuid in metadata_uuids]
        metadata_service.fetch_resource = Mock(side_effect=metadata_resources)

        # and:
        cloud_urls = [f'https://upload.tld/metadata{i}.json' for i in range(0, 3)]
        staging_details = [StagingInfo(cloud_url=url) for url in cloud_urls]
        staging_service.stage_update = Mock(side_effect=staging_details)

        # and:
        bundle_uuid = '9dfca176-0ddf-4384-8b71-b74237edb8be'
        bundle_files = [{'uuid': uuid} for uuid in metadata_uuids]
        bundle = Bundle(source={'bundle': {'uuid': bundle_uuid, 'files': bundle_files}})
        bundle = Mock(wraps=bundle)
        bundle_service.fetch = Mock(return_value=bundle)

        # and:
        metadata_urls = [f'https://data.hca.tld/metadata/{uuid}' for uuid in metadata_uuids]
        staging_area_uuid = '947a6528-184e-4a05-9af5-355e1f450609'
        update_submission = {'stagingDetails': {'stagingAreaUuid': {'uuid': staging_area_uuid}}}
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
