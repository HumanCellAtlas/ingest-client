from unittest import TestCase

from broker.ingestexportservice import IngestExporter


class TestExporter(TestCase):

    def test_bundleProject(self):
        # given:
        exporter = IngestExporter()

        # and:
        project_entity = {
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
