from unittest import TestCase

from broker.ingestexportservice import IngestExporter

class TestExporter(TestCase):

    def test_bundleProject(self):
        # given:
        exporter = IngestExporter()
        project_entity = self._create_entity_template()

        # when:
        project_bundle = exporter.bundleProject(project_entity)

        # then:
        self.assertEqual(project_bundle['content'], project_entity['content'])
        self.assertEqual(project_bundle['schema_version'], exporter.schema_version)
        self.assertEqual(project_bundle['schema_type'], 'project_bundle')

        # and:
        schema_ref = project_bundle['describedBy']
        self.assertTrue(schema_ref.endswith('/project.json'))
        self.assertTrue(schema_ref.startswith(exporter.schema_url))

        # and:
        hca_ingest = project_bundle['hca_ingest']
        self.assertEqual(hca_ingest['submissionDate'], project_entity['submissionDate'])
        self.assertEqual(hca_ingest['updateDate'], project_entity['updateDate'])
        self.assertEqual(hca_ingest['document_id'], project_entity['uuid']['uuid'])
        self.assertEqual(hca_ingest['accession'], project_entity['accession'])

        # and:
        self.assertFalse('content' in hca_ingest.keys())

    def test_bundleFileIngest(self):
        # given:
        exporter = IngestExporter()
        file_entity = self._create_entity_template()

        # and:
        file_specific_details = {
            'fileName': 'SRR3934351_1.fastq.gz',
            'cloudUrl': 'https://sample.com/path/to/file',
            'checksums': [],
            'validationId': '62d900'
        }
        file_entity.update(file_specific_details)

        # when:
        file_ingest = exporter.bundleFileIngest(file_entity)

        # then:
        self.assertEqual(file_ingest['content'], file_entity['content'])

        # and:
        hca_ingest = file_ingest['hca_ingest']
        self.assertEqual(hca_ingest['document_id'], file_entity['uuid']['uuid'])
        self.assertEqual(hca_ingest['submissionDate'], file_entity['submissionDate'])
        #TODO is describedBy required?

    def test_bundleProtocolIngest(self):
        # given:
        exporter = IngestExporter()

        # and:
        protocol_entity = self._create_entity_template()

        # when:
        protocol_ingest = exporter.bundleProtocolIngest(protocol_entity)

        # then:
        self.assertEqual(protocol_ingest['hca_ingest']['document_id'], protocol_entity['uuid']['uuid'])

    def _create_entity_template(self):
        return {
            'submissionDate': '2018-03-14T09:53:02Z',
            'updateDate': '2018-03-14T09:53:02Z',
            'content': {},
            '_links': [],
            'events': [],
            'validationState': 'valid',
            'validationErrors': [],
            'uuid': {
                'uuid': '4674424e-3ab1-491c-8295-a68c7bb04b61'
            },
            'accession': 'accession123'
        }