from unittest import TestCase

from mock import Mock

from ingest.exporter.metadata import MetadataResource, MetadataService, MetadataParseException, MetadataProvenance


class MetadataResourceTest(TestCase):

    def test_provenance_from_dict(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = {
            'uuid': {'uuid': uuid_value},
            'submissionDate': 'a submission date',
            'updateDate': 'an update date',
            'dcpVersion': '2019-12-02T13:40:50.520Z',
            'content': {
                'describedBy': 'https://some-schema/1.2.3'
            }
        }

        # when:
        metadata_provenance = MetadataResource.provenance_from_dict(data)

        # then:
        self.assertIsNotNone(metadata_provenance)
        self.assertEqual(uuid_value, metadata_provenance.document_id)
        self.assertEqual('a submission date', metadata_provenance.submission_date)
        self.assertEqual('2019-12-02T13:40:50.520Z', metadata_provenance.update_date)

    def test_provenance_from_dict_fail_fast(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = {'uuid': uuid_value,  # unexpected structure structure
                'submissionDate': 'a submission date',
                'updateDate': 'an update date'}

        # then:
        with self.assertRaises(MetadataParseException):
            # when
            MetadataResource.provenance_from_dict(data)

    def test_from_dict(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = self._create_test_data(uuid_value)

        # when:
        metadata = MetadataResource.from_dict(data)

        # then:
        self.assertIsNotNone(metadata)
        self.assertEqual('biomaterial', metadata.metadata_type)
        self.assertEqual(data['content'], metadata.metadata_json)
        self.assertEqual(data['dcpVersion'], metadata.dcp_version)

        # and:
        self.assertEqual(uuid_value, metadata.uuid)

    def test_from_dict_provenance_optional(self):
        # given:
        uuid = '566be204-a684-4896-bda7-8dbb3e4fc65c'
        data_no_provenance = self._create_test_data(uuid)
        del data_no_provenance['submissionDate']

        # and:
        data = self._create_test_data(uuid)

        # when:
        metadata_no_provenance = MetadataResource.from_dict(data_no_provenance, require_provenance=False)
        metadata = MetadataResource.from_dict(data, require_provenance=False)

        # then:
        self.assertIsNotNone(metadata_no_provenance)
        self.assertEqual(uuid, metadata_no_provenance.uuid)
        self.assertIsNone(metadata_no_provenance.provenance)

        # and:
        self.assertIsNotNone(metadata.provenance)

    def test_from_dict_fail_fast_with_missing_info(self):
        # given:
        data = {}
        # then:
        with self.assertRaises(MetadataParseException):
            # when
            MetadataResource.from_dict(data)

    def test_to_bundle_metadata(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = self._create_test_data(uuid_value)
        metadata = MetadataResource.from_dict(data)

        # and:
        data_no_provenance = self._create_test_data(uuid_value)
        del data_no_provenance['submissionDate']
        metadata_no_provenance = MetadataResource.from_dict(data_no_provenance, require_provenance=False)
        self.assertIsNone(metadata_no_provenance.provenance)

        # when
        bundle_metadata = metadata.to_bundle_metadata()
        bundle_metadata_no_provenance = metadata_no_provenance.to_bundle_metadata()

        # then:
        self.assertTrue('provenance' in bundle_metadata)
        self.assertTrue(bundle_metadata['provenance'] == metadata.provenance.to_dict())
        self.assertTrue(set(data['content'].keys()) <= set(
            bundle_metadata.keys()))  # <= operator checks if a dict is subset of another dict

        # and:
        self.assertIsNotNone(bundle_metadata_no_provenance)
        self.assertEqual(metadata_no_provenance.metadata_json['describedBy'],
                         bundle_metadata_no_provenance['describedBy'])

    @staticmethod
    def _create_test_data(uuid_value):
        return {'type': 'Biomaterial',
                'uuid': {'uuid': uuid_value},
                'content': {'describedBy': "http://some-schema/1.2.3",
                            'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                'dcpVersion': '6.9.1',
                'submissionDate': 'a date',
                'updateDate': 'another date'}

    def test_get_staging_file_name(self):
        # given:
        metadata_resource_1 = MetadataResource(metadata_type='specimen',
                                               uuid='9b159cae-a1fe-4cce-94bc-146e4aa20553',
                                               metadata_json={'description': 'test'},
                                               dcp_version='5.1.0',
                                               provenance=MetadataProvenance('9b159cae-a1fe-4cce-94bc-146e4aa20553',
                                                                             'some date', 'some other date', 1, 1))
        metadata_resource_2 = MetadataResource(metadata_type='donor_organism',
                                               uuid='38e0ee7c-90dc-438a-a0ed-071f9231f590',
                                               metadata_json={'text': 'sample'},
                                               dcp_version='1.0.7',
                                               provenance=MetadataProvenance('38e0ee7c-90dc-438a-a0ed-071f9231f590',
                                                                             'some date', 'some other date', '2', '2'))

        # expect:
        self.assertEqual('specimen_9b159cae-a1fe-4cce-94bc-146e4aa20553.json',
                         metadata_resource_1.get_staging_file_name())
        self.assertEqual('donor_organism_38e0ee7c-90dc-438a-a0ed-071f9231f590.json',
                         metadata_resource_2.get_staging_file_name())


class MetadataServiceTest(TestCase):

    def test_fetch_resource(self):
        # given:
        ingest_client = Mock(name='ingest_client')
        uuid = '301636f7-f97b-4379-bf77-c5dcd9f17bcb'
        raw_metadata = {'type': 'Biomaterial',
                        'uuid': {'uuid': uuid},
                        'content': {'describedBy': "http://some-schema/1.2.3",
                                    'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                        'dcpVersion': '2019-12-02T13:40:50.520Z',
                        'submissionDate': 'a submission date',
                        'updateDate': 'an update date'
                        }
        ingest_client.get_entity_by_callback_link = Mock(return_value=raw_metadata)

        # and:
        metadata_service = MetadataService(ingest_client)

        # when:
        metadata_resource = metadata_service.fetch_resource(
            'hca.domain.com/api/cellsuspensions/301636f7-f97b-4379-bf77-c5dcd9f17bcb')

        # then:
        self.assertEqual('biomaterial', metadata_resource.metadata_type)
        self.assertEqual(uuid, metadata_resource.uuid)
        self.assertEqual(raw_metadata['content'], metadata_resource.metadata_json)
        self.assertEqual(raw_metadata['dcpVersion'], metadata_resource.dcp_version)
        self.assertEqual(raw_metadata['submissionDate'], metadata_resource.provenance.submission_date)
        self.assertEqual(raw_metadata['dcpVersion'], metadata_resource.provenance.update_date)
