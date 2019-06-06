from unittest import TestCase

from mock import Mock

from ingest.api.stagingapi import FileDescription
from ingest.exporter.bundle_update_service import BundleUpdateService, MetadataResource, \
    StagingService, MetadataService, Bundle


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
        bundle = Bundle(source={'files': [file_1, file_2]})

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

    def test_update_file(self):
        # given:
        uuid = '5a583ae9-2a28-4d6d-8109-7e47c56bd5ad'
        bundle_file = _create_test_bundle_file(uuid=uuid, name='cell_suspension_0.json',
                                               version='2019-06-05T160722.098000Z', indexed=False)
        bundle = Bundle(source={'files': [bundle_file]})

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
        self.assertEqual('application/json; dcp-type="cell_suspension_x"',
                         updated_file.get('content-type'))


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
        test_file_description = FileDescription(['chks0mz'], 'application/json', 'file.name', 1024,
                                                'domain.com/file.url')
        staging_client.stageFile = Mock(return_value=test_file_description)

        # and:
        staging_service = StagingService(staging_client)

        # when:
        staging_area_uuid = '7455716e-9639-41d9-bff9-d763f9ee028d'
        file_description = staging_service.stage_update(staging_area_uuid, metadata_resource)

        # then:
        self.assertEqual(test_file_description, file_description)
        staging_client.stageFile.assert_called_once_with(staging_area_uuid,
                                                         metadata_resource.get_staging_file_name(),
                                                         metadata_resource.metadata_json,
                                                         metadata_resource.metadata_type)


class BundleUpdateServiceTest(TestCase):

    def test_update_bundle(self):
        # given:
        dss_client = Mock(name='dss_client')

        metadata_service = Mock(name='metadata_service')
        staging_service = Mock(name='staging_service')
        service = BundleUpdateService(metadata_service, staging_service)

        # and:
        test_metadata = MetadataResource(metadata_type='donor_organism',
                                         uuid='16e3bd3f-34e8-40e6-90b9-9cbfa9b03cf5',
                                         metadata_json={'name': 'sample', 'description': 'test'},
                                         dcp_version='5.2.1')
        metadata_service.fetch_resource = Mock(return_value=test_metadata)

        # and:
        test_file_description = FileDescription(['chkz0m'], 'file', 'sample.ss2', 1024,
                                                'sample.url')
        staging_service.stage_update = Mock(return_value=test_file_description)

        # and:
        updated_bundle = {'files': [{}]}
        dss_client.get_bundle = Mock(return_value=updated_bundle)
        dss_client.create_file = Mock(return_value={})
        dss_client.put_bundle = Mock(return_value={})

        # when:
        update_submission = {'stagingDetails': {'stagingAreaUuid': {'uuid': '3cce991'}}}
        callback_list = ['ingest-api/23cb771']
        updated_bundle = service.update_bundle(update_submission, '67c9d90', 'v3', callback_list)

        # expect:
        self.assertIsNotNone(updated_bundle)
