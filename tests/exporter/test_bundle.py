from unittest import TestCase
from uuid import uuid4

from mock import Mock, call

from ingest.exporter.bundle import Bundle, BundleService, BundleManifest
from ingest.exporter.metadata import MetadataResource, MetadataProvenance
from ingest.exporter.staging import StagingInfo
from tests.exporter.test_exporter import _create_test_bundle_file


class BundleManifestTest(TestCase):

    def test_add_bundle_file_biomaterial(self):
        # given:
        manifest = BundleManifest()

        # when:
        biomaterial_uuid = '0a917e84-929b-4a8b-8e26-821eb7b30371'
        manifest.add_bundle_file('biomaterial', {biomaterial_uuid: [biomaterial_uuid]})

        # expect:
        self.assertEqual([biomaterial_uuid], manifest.fileBiomaterialMap.get(biomaterial_uuid))

    def test_add_bundle_file_process(self):
        # given:
        manifest = BundleManifest()

        # when:
        process_uuids = ['9fd82f90-274c-425c-a755-9e97f301e65f',
                         '0903d7f7-fbfc-465b-ae81-5c37b6bb4bbc',
                         '84c122a0-56ad-47e5-ae33-1558251eb8f9']
        for process_uuid in process_uuids:
            manifest.add_bundle_file('process', {process_uuid: [process_uuid]})

        # expect:
        for process_uuid in process_uuids:
            self.assertEqual([process_uuid], manifest.fileProcessMap.get(process_uuid))

    def test_add_bundle_file_file(self):
        # given:
        manifest = BundleManifest()

        # when:
        file_uuid = '4e2c6ffb-145e-4e83-bc1f-bf9842bc6d8e'
        manifest.add_bundle_file('file', {file_uuid: [file_uuid]})

        # then:
        self.assertEqual([file_uuid], manifest.fileFilesMap.get(file_uuid))

    def test_add_bundle_file_project(self):
        # given:
        manifest = BundleManifest()

        # and:
        project_uuid = '4ae09cb7-4596-4158-86e2-755f7eb9afff'
        manifest.add_bundle_file('project', {project_uuid: [project_uuid]})

        # expect:
        self.assertEqual([project_uuid], manifest.fileProjectMap.get(project_uuid))

    def test_add_bundle_file_protocol(self):
        # given:
        manifest = BundleManifest()

        # when:
        protocol_uuids = ['bf601206-26f6-4fba-90ff-86a34a5d9cfd',
                          '58ef9295-e8ed-46c7-bd5c-af6b67ffb17b']
        for protocol_uuid in protocol_uuids:
            manifest.add_bundle_file('protocol', {protocol_uuid: [protocol_uuid]})

        # expect:
        self.assertEqual([protocol_uuids[0]], manifest.fileProtocolMap.get(protocol_uuids[0]))
        self.assertEqual([protocol_uuids[1]], manifest.fileProtocolMap.get(protocol_uuids[1]))

    def test_add_bundle_file_ignore_links(self):
        # given:
        manifest = BundleManifest()

        # when:
        links_uuid = '085431d7-a40d-4d25-8c71-a077a69acf62'
        manifest.add_bundle_file('links', {links_uuid: [links_uuid]})

        # then:
        map_names = ['biomaterial', 'files', 'process', 'project', 'protocol']
        for file_map_name in [f'file{name.capitalize()}Map' for name in map_names]:
            file_map = getattr(manifest, file_map_name)
            self.assertEqual(0, len(file_map), f'Expected [{file_map_name}] to be empty.')

    def test_add_bundle_file_data(self):
        # given:
        manifest = BundleManifest()

        # when:
        file_uuids = ['2141b403-7b3a-4e5c-83af-05c6c8d5ab9a',
                      '1830d1c9-5c55-4586-be1b-eee16fea0c51',
                      'c3b4dee4-a6d0-41b0-b1b4-726d95995d4d']
        for file_uuid in file_uuids:
            manifest.add_bundle_file('data', {file_uuid: [file_uuid]})

        # then:
        self.assertEqual(file_uuids, manifest.dataFiles)


def _create_test_bundle_source(bundle_uuid, bundle_version, metadata_files, creator_uid):
    bundle_source = {
        'bundle': {
            'creator_uid': creator_uid,
            'uuid': bundle_uuid,
            'version': bundle_version,
            'files': metadata_files
        }
    }
    return bundle_source


class BundleTest(TestCase):

    def test_create_from_source(self):
        # given
        creator_uid = 5050
        metadata_uuid = 'af690c04-8562-4680-bbf4-56bb9f58b265'
        metadata_file = _create_test_bundle_file(uuid=metadata_uuid,
                                                 content_type='biomaterial')

        bundle_uuid = '93d3d2be-36e0-465e-b37f-0f48a998630e'
        bundle_version = '2019-06-14T102922.001122Z'
        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, [metadata_file], creator_uid)

        # when
        bundle = Bundle.bundle_from_source(bundle_source)

        # expect:
        self.assertEqual(bundle.uuid, bundle_uuid)
        self.assertEqual(bundle.creator_uid, creator_uid)
        self.assertEqual(bundle.version, bundle_version)
        self.assertTrue(metadata_uuid in bundle.files)

    def test_get_file(self):
        # given:
        creator_uid = 5050
        bundle_uuid = '93d3d2be-36e0-465e-b37f-0f48a998630e'
        bundle_version = '2019-06-14T102922.001122Z'
        file_1 = _create_test_bundle_file(uuid='15b22393-cc48-4dff-bc19-22c189d7c35e',
                                          name='donor_organism_0.json',
                                          version='2019-06-06T1033833.011000Z')
        file_2 = _create_test_bundle_file(uuid='b56f4e1f-3651-463e-8e6f-b931ea5f21a2',
                                          name='donor_organism_1.json',
                                          version='2019-06-06T1034255.023000Z')

        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, [file_1, file_2], creator_uid)
        bundle = Bundle.bundle_from_source(bundle_source)

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
        bundle = Bundle('some-uuid', {}, original_version, 8000)
        self.assertEqual(original_version, bundle.get_version())

        # when:
        updated_version = '2019-06-10T141005.279000Z'
        bundle.update_version(updated_version)

        # then:
        self.assertEqual(updated_version, bundle.get_version())

    def test_update_file(self):
        # given:
        creator_uid = 5050
        bundle_uuid = '93d3d2be-36e0-465e-b37f-0f48a998630e'
        bundle_version = '2019-06-14T102922.001122Z'

        uuid = '5a583ae9-2a28-4d6d-8109-7e47c56bd5ad'
        bundle_file = _create_test_bundle_file(uuid=uuid, name='cell_suspension_0.json',
                                               version='2019-06-05T160722.098000Z', indexed=False)

        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, [bundle_file], creator_uid)
        bundle = Bundle.bundle_from_source(bundle_source)

        # and:
        metadata_resource = MetadataResource(metadata_type='cell_suspension_x',
                                             uuid=uuid,
                                             metadata_json={'text': 'description'},
                                             dcp_version='2019-06-12T14:14:14.077Z',
                                             provenance=MetadataProvenance("", "", "", "", ""))

        # when:
        bundle.update_file(metadata_resource)

        # then:
        updated_file = bundle.get_file(uuid)
        self.assertEqual('2019-06-12T141414.077000Z', updated_file.get('version'))
        self.assertEqual('application/json; dcp-type="metadata/cell_suspension_x"',
                         updated_file.get('content-type'))

    def test_generate_manifest(self):
        # given:
        metadata_uuid = 'af690c04-8562-4680-bbf4-56bb9f58b265'
        metadata_file = _create_test_bundle_file(uuid=metadata_uuid,
                                                 content_type='biomaterial')

        # and:
        bundle_uuid = '93d3d2be-36e0-465e-b37f-0f48a998630e'
        bundle_version = '2019-06-14T102922.001122Z'
        creator_uid = 5050

        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, [metadata_file], creator_uid)
        bundle = Bundle.bundle_from_source(bundle_source)

        # when:
        envelope_uuid = 'c1cec0de-d72b-40d6-a536-f47d922518b2'
        manifest = bundle.generate_manifest(envelope_uuid)

        # then:
        self.assertEqual(bundle_uuid, manifest.bundleUuid)
        self.assertEqual(bundle_version, manifest.bundleVersion)
        self.assertEqual(envelope_uuid, manifest.envelopeUuid)

    _bundle_attr_map = {
        'project': 'fileProjectMap',
        'biomaterial': 'fileBiomaterialMap',
        'file': 'fileFilesMap',
        'process': 'fileProcessMap',
        'protocol': 'fileProtocolMap'
    }

    def test_generate_manifest_with_correct_file_maps(self):
        def random_uuids(count):
            return [str(uuid4()) for _ in range(0, count)]

        self._do_test_generate_manifest('project', random_uuids(1))
        self._do_test_generate_manifest('biomaterial', random_uuids(3))
        self._do_test_generate_manifest('file', random_uuids(2))
        self._do_test_generate_manifest('process', random_uuids(5))
        self._do_test_generate_manifest('protocol', random_uuids(1))

    def _do_test_generate_manifest(self, metadata_type: str, file_uuids: list):
        # given:
        metadata_files = [_create_test_bundle_file(uuid=file_uuid, content_type=metadata_type)
                          for file_uuid in file_uuids]

        # and:
        bundle_uuid = '93d3d2be-36e0-465e-b37f-0f48a998630e'
        bundle_version = '2019-06-14T102922.001122Z'
        creator_uid = 5050
        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, metadata_files, creator_uid)
        bundle = Bundle.bundle_from_source(bundle_source)

        # when:
        manifest = bundle.generate_manifest('cffaa257-71ab-422f-bdb9-2a696b68bb33')

        # then:
        entry_count = len(file_uuids)
        file_map_attr = self._bundle_attr_map.get(metadata_type)
        metadata_file_map = getattr(manifest, file_map_attr)
        self.assertEqual(entry_count, len(metadata_file_map),
                         f'expecting [{entry_count}] entries in {file_map_attr}')

        # and: verify correct map entries
        for file_uuid, file_map_values in metadata_file_map.items():
            self.assertEqual(1, len(file_map_values))
            self.assertTrue(file_uuid in file_map_values)

        # and:
        self._assert_maps_are_empty_except(manifest, metadata_type)

    def _assert_maps_are_empty_except(self, bundle_manifest: BundleManifest, target_type: str):
        for metadata_type, map_name in self._bundle_attr_map.items():
            if metadata_type != target_type:
                bundle_file_map = getattr(bundle_manifest, map_name)
                self.assertEqual(0, len(bundle_file_map), f'Expected [{map_name}] to be empty.')

    def test_generate_manifest_with_correct_data_files(self):
        # given:
        data_file_uuids = ['e847292e-fecc-46f3-afb4-63639f38b119',
                           '03ac1223-ff49-4f5b-be3d-bb3c49bcbbeb']
        data_files = [_create_test_bundle_file(uuid=file_uuid, name=f'{file_uuid}_file.json',
                                               content_type_prefix='', content_type='data',
                                               version='2019-06-20T134800.000000')
                      for file_uuid in data_file_uuids]

        # and:
        bundle_uuid = '4149ef7d-c4ad-47b5-b053-0cfb7d14c597'
        bundle_version = '2019-06-21T133113.313313Z'
        creator_uid = 5050
        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, data_files, creator_uid)
        bundle = Bundle.bundle_from_source(bundle_source)

        # when:
        envelope_uuid = '7416da14-8535-4923-80d9-a7b0b062d70e'
        manifest = bundle.generate_manifest(envelope_uuid)

        # then:
        self.assertEqual(data_file_uuids, manifest.dataFiles)


class BundleServiceTest(TestCase):

    def test_fetch(self):
        # given:
        dss_client = Mock(name='dss_client')
        bundle_uuid = '69f92d53-0d25-4577-948d-dad76dd190cc'
        bundle_version = '2019-06-21T133113.313313Z'
        creator_uid = 5050
        test_file = _create_test_bundle_file(uuid='0c43ccf0-04ed-41af-9213-be3bcae14c06')
        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, [test_file], creator_uid)
        dss_client.get_bundle = Mock(return_value=bundle_source)

        # and:
        service = BundleService(dss_client)

        # when:
        bundle = service.fetch(bundle_uuid)

        # then:
        self.assertTrue(type(bundle) is Bundle, 'get_bundle should return a Bundle.')
        self.assertEqual(bundle_uuid, bundle.uuid)

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
        creator_uid = 5050
        file_1 = _create_test_bundle_file(uuid=uuid_1, version='2019-06-07T170321.000000Z')
        file_2 = _create_test_bundle_file(uuid=uuid_2, version='2019-06-01T103033.000000Z')
        unchanged_uuid = '66a9da69-62c0-4792-9e89-09235d5e68e1'
        unchanged_file = _create_test_bundle_file(uuid=unchanged_uuid,
                                                  version='2019-06-06T112935.000000Z')
        bundle_version = '2019-06-07T220321.010000Z'

        bundle_source = _create_test_bundle_source(bundle_uuid, bundle_version, [file_1, file_2, unchanged_file],
                                                   creator_uid)
        bundle = Bundle.bundle_from_source(bundle_source)

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
