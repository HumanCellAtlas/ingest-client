from copy import deepcopy
from unittest import TestCase

from mock import Mock, call

from ingest.exporter.bundle import Bundle, BundleService
from ingest.exporter.metadata import MetadataResource
from ingest.exporter.staging import StagingInfo
from tests.exporter.test_exporter import _create_test_bundle_file


class BundleTest(TestCase):

    def test_create(self):
        # when:
        uuid = '6fff72c7-0a05-419f-8c78-5c3dd32cea29'
        creator_uid = 5050
        version = '2019-06-13T144433.222111Z'
        bundle = Bundle.create(uuid, creator_uid, version)

        # expect:
        self.assertEqual(uuid, bundle.uuid)
        self.assertEqual(creator_uid, bundle.creator_uid)
        self.assertEqual(version, bundle.get_version())

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
                                             dcp_version='2019-06-12T14:14:14.077Z')

        # when:
        bundle.update_file(metadata_resource)

        # then:
        updated_file = bundle.get_file(uuid)
        self.assertEqual('2019-06-12T141414.077000Z', updated_file.get('version'))
        self.assertEqual('metadata/cell_suspension_x', updated_file.get('content-type'))

    def test_generate_manifest(self):
        # given:
        biomaterial_uuid = 'af690c04-8562-4680-bbf4-56bb9f58b265'
        biomaterial_file = _create_test_bundle_file(uuid=biomaterial_uuid,
                                                    content_type='biomaterial')

        # and:
        bundle_uuid = '93d3d2be-36e0-465e-b37f-0f48a998630e'
        bundle_version = '2019-06-14T102922.001122Z'
        bundle = Bundle(source={'bundle': {
            'uuid': bundle_uuid,
            'version': bundle_version,
            'files': [biomaterial_file]
        }})

        # when:
        envelope_uuid = 'c1cec0de-d72b-40d6-a536-f47d922518b2'
        manifest = bundle.generate_manifest(envelope_uuid)

        # then:
        self.assertEqual(bundle_uuid, manifest.bundleUuid)
        self.assertEqual(bundle_version, manifest.bundleVersion)

        # and: uuid is 2 branches deep
        manifest_envelope_uuid = manifest.envelopeUuid.get('uuid')
        self.assertIsNotNone(manifest_envelope_uuid)
        self.assertEqual(envelope_uuid, manifest_envelope_uuid.get('uuid'))


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
        unchanged_uuid = '66a9da69-62c0-4792-9e89-09235d5e68e1'
        unchanged_file = _create_test_bundle_file(uuid=unchanged_uuid,
                                                  version='2019-06-06T112935.000000Z')
        bundle_version = '2019-06-07T220321.010000Z'
        bundle = Bundle(source={'bundle': {'uuid': bundle_uuid, 'version': bundle_version,
                                           'files': [file_1, file_2, unchanged_file]}})

        # when:
        service.update(bundle, [staging_info_1, staging_info_2])

        # then:
        dss_client.put_file.assert_has_calls(
            [call(None, {'url': staging_info_1.cloud_url, 'dss_uuid': uuid_1,
                         'update_date': file_1['version']}),
             call(None, {'url': staging_info_2.cloud_url, 'dss_uuid': uuid_2,
                         'update_date': file_2['version']})], any_order=True)

        # and:
        expected_files = [file_1, file_2, unchanged_file]
        dss_client.put_bundle.assert_called_once_with(bundle_uuid, bundle_version, expected_files)