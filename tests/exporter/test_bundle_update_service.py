from unittest import TestCase

from mock import Mock, call

from ingest.api.stagingapi import FileDescription
from ingest.exporter.bundle_update_service import BundleUpdateService, MetadataResource, \
    StagingService, MetadataService, Bundle, BundleService, StagingInfo, Exporter


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
        self.assertEqual('9b159cae-a1fe-4cce-94bc-146e4aa20553.5.1.0.json',
                         metadata_resource_1.get_staging_file_name())
        self.assertEqual('38e0ee7c-90dc-438a-a0ed-071f9231f590.1.0.7.json',
                         metadata_resource_2.get_staging_file_name())


def _create_test_bundle_file(uuid='', name='', content_type='biomaterial', version='',
                             indexed=True):
    return {'content-type': f'application/json; dcp-type="{content_type}"', 'uuid': uuid,
            'name': name, 'version': version, 'indexed': indexed}


class BundleTest(TestCase):

    def test_get_file(self):
        # given:
        file_1 = _create_test_bundle_file(uuid='15b22393-cc48-4dff-bc19-22c189d7c35e',
                                          name='donor_organism_0.json',
                                          version='2019-06-06T1033833.011000Z')
        file_2 = _create_test_bundle_file(uuid='b56f4e1f-3651-463e-8e6f-b931ea5f21a2',
                                          name='donor_organism_1.json',
                                          version='2019-06-06T1034255.023000Z')
        bundle = Bundle(source={'bundle': {'files': [file_1, file_2]}})

        # when:
        target_file = bundle.get_file(file_2['uuid'])

        # then:
        # In case you're wondering why on the left we use indexing while on the right we use get:
        # On the left, we can assert existence of the field from object we create;
        # on the right, we can't as that's what the system-under-test gives us.
        # This also prevents the scenario where the check for equality passes because get's
        # return None, i.e. file_2.get('non-existent') == target_file.get('non-existent)'.
        self.assertEqual(file_2['uuid'], target_file.get('uuid'))
        self.assertEqual(file_2['name'], target_file.get('name'))
        self.assertEqual(file_2['content-type'], target_file.get('content-type'))
        self.assertEqual(file_2['version'], target_file.get('version'))
        self.assertEqual(file_2['indexed'], target_file.get('indexed'))

    def test_update_version(self):
        # given:
        original_version = '2019-06-03T134835.366000Z'
        bundle = Bundle(source={'bundle': {'version': original_version}})
        self.assertEqual(original_version, bundle.get_version())

        # when:
        updated_version = '2019-06-10T141005.279000Z'
        bundle.update_version(updated_version)

        # then:
        self.assertEqual(updated_version, bundle.get_version())

    def test_update_file(self):
        # given:
        uuid = '5a583ae9-2a28-4d6d-8109-7e47c56bd5ad'
        bundle_file = _create_test_bundle_file(uuid=uuid, name='cell_suspension_0.json',
                                               version='2019-06-05T160722.098000Z', indexed=False)
        bundle = Bundle(source={'bundle': {'files': [bundle_file]}})

        # and:
        metadata_resource = MetadataResource(metadata_type='cell_suspension_x',
                                             uuid=uuid,
                                             metadata_json={'text': 'description'},
                                             dcp_version='2019-06-12T141414.077000Z')

        # when:
        bundle.update_file(metadata_resource)

        # then:
        updated_file = bundle.get_file(uuid)
        self.assertEqual(metadata_resource.dcp_version, updated_file.get('version'))
        self.assertEqual('metadata/cell_suspension_x', updated_file.get('content-type'))


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


class StagingServiceTest(TestCase):

    def test_stage_update(self):
        # given:
        metadata_resource = MetadataResource(metadata_type='donor_organism',
                                             uuid='831d4b6e-e8a2-42ce-b7c0-8d6ffcc15370',
                                             metadata_json={'description': 'test'},
                                             dcp_version='4.2.1')

        # and:
        staging_client = Mock(name='staging_client')
        file_description = FileDescription(['chks0mz'], 'application/json', 'file.name', 1024,
                                                'http://domain.com/file.url')
        staging_client.stageFile = Mock(return_value=file_description)

        # and:
        staging_service = StagingService(staging_client)

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        staging_info = staging_service.stage_update(staging_area_uuid, metadata_resource)

        # then:
        self.assertTrue(type(staging_info) is StagingInfo,
                        'stage_update should return StagingRecord.')
        self.assertEqual(metadata_resource.uuid, staging_info.metadata_uuid)
        self.assertEqual(file_description.name, staging_info.file_name)
        self.assertEqual(file_description.url, staging_info.cloud_url)

        # and:
        staging_client.stageFile.assert_called_once_with(staging_area_uuid,
                                                         metadata_resource.get_staging_file_name(),
                                                         metadata_resource.metadata_json,
                                                         metadata_resource.metadata_type)


class BundleServiceTest(TestCase):

    def test_fetch(self):
        # given:
        dss_client = Mock(name='dss_client')
        uuid = '69f92d53-0d25-4577-948d-dad76dd190cc'
        test_file = _create_test_bundle_file(uuid='0c43ccf0-04ed-41af-9213-be3bcae14c06')
        bundle_source = {'bundle': {'uuid': uuid, 'files': [test_file]}}
        dss_client.get_bundle = Mock(return_value=bundle_source)

        # and:
        service = BundleService(dss_client)

        # when:
        bundle = service.fetch(uuid)

        # then:
        self.assertTrue(type(bundle) is Bundle, 'get_bundle should return a Bundle.')
        self.assertEqual(uuid, bundle.uuid)

        # and:
        self.assertEqual(1, bundle.count_files())
        self.assertEqual(test_file, bundle.get_file(test_file['uuid']))

    def test_update(self):
        # given:
        dss_client = Mock(name='dss_client')
        service = BundleService(dss_client)

        # and:
        uuid_1 = '51e37d20-6dc4-41e4-9ae5-36e0c6d4c1e6'
        staging_info_1 = StagingInfo(metadata_uuid=uuid_1, file_name='cell_suspension_0.json',
                                     cloud_url='http://sample.tld/files/file0.json')
        uuid_2 = 'ab81c860-b114-484a-a134-3923a7e0041b'
        staging_info_2 = StagingInfo(metadata_uuid=uuid_2, file_name='cell_suspension_1.json',
                                     cloud_url='http://sample.tld/files/file1.json')

        # and:
        bundle_uuid = '25f26f33-9413-45e0-b83c-979ce59cef62'
        file_1 = _create_test_bundle_file(uuid=uuid_1, version='2019-06-07T170321.000000Z')
        file_2 = _create_test_bundle_file(uuid=uuid_2, version='2019-06-01T103033.000000Z')
        bundle_version = '2019-06-07T220321.010000Z'
        bundle = Bundle(source={'bundle': {'uuid': bundle_uuid, 'files': [file_1, file_2],
                                           'version': bundle_version}})

        # when:
        service.update(bundle, [staging_info_1, staging_info_2])

        # then:
        dss_client.put_file.assert_has_calls(
            [call(None, {'url': staging_info_1.cloud_url, 'dss_uuid': uuid_1,
                         'update_date': file_1['version']}),
             call(None, {'url': staging_info_2.cloud_url, 'dss_uuid': uuid_2,
                         'update_date': file_2['version']})], any_order=True)

        # and:
        dss_client.put_bundle.assert_called_once_with(bundle_uuid, bundle_version, [file_1, file_2])


class BundleUpdateServiceTest(TestCase):

    def test_update_bundle(self):
        # given:
        metadata_service = Mock(name='metadata_service')
        staging_service = Mock(name='staging_service')
        dss_client = Mock(name='dss_client')
        service = BundleUpdateService(metadata_service, staging_service, dss_client)

        # and:
        bundle_file_uuid = '16e3bd3f-34e8-40e6-90b9-9cbfa9b03cf5'
        test_metadata = MetadataResource(metadata_type='donor_organism',
                                         uuid=bundle_file_uuid,
                                         metadata_json={'name': 'sample', 'description': 'test'},
                                         dcp_version='5.2.1')
        metadata_service.fetch_resource = Mock(return_value=test_metadata)

        # and:
        test_file_description = FileDescription(['chkz0m'], 'file', 'sample.ss2', 1024,
                                                'sample.url')
        staging_service.stage_update = Mock(return_value=test_file_description)

        # and:
        bundle_file = Mock(wraps=_create_test_bundle_file(uuid=bundle_file_uuid))
        dss_client.fetch = Mock(return_value={'bundle': {'files': [bundle_file]}})

        # when:
        update_submission = {'stagingDetails': {'stagingAreaUuid': {'uuid': '3cce991'}}}
        callback_list = ['http://api.ingest.tld/metadata/23cb771']
        updated_bundle = service.update_bundle(update_submission, '67c9d90', 'v3', callback_list)

        # then:
        self.assertIsNotNone(updated_bundle)


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
        bundle.update_file.assert_has_calls([call(metadata) for metadata in metadata_resources],
                                            any_order=True)
        bundle_service.update.assert_called_once_with(bundle, staging_details)
