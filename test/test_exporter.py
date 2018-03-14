from unittest import TestCase

from broker.ingestexportservice import IngestExporter


class TestExporter(TestCase):

    def test_processProjectBundle(self):
        # given:
        exporter = IngestExporter()

        # and:
        uuid = '4674424e-3ab1-491c-8295-a68c7bb04b61'
        accession_id = 'accession123'
        project_entity = {
            'submissionDate': '2018-03-14T09:53:02Z',
            'updateDate': '2018-03-14T09:53:02Z',
            'content': {},
            '_links': [],
            'events': [],
            'validationState': 'valid',
            'validationErrors': [],
            'uuid': {
                'uuid': uuid
            },
            'accession': accession_id
        }

        # when:
        project_bundle = exporter.processProjectBundle(project_entity)

        # then:
        self.assertEqual(project_bundle['content'], project_entity['content'])

        # and:
        hca_ingest = project_bundle['hca_ingest']
        self.assertEqual(hca_ingest['submissionDate'], project_entity['submissionDate'])
        self.assertEqual(hca_ingest['updateDate'], project_entity['updateDate'])
        self.assertEqual(hca_ingest['document_id'], uuid)
        self.assertEqual(hca_ingest['accession'], accession_id)

        # and:
        self.assertFalse('content' in hca_ingest.keys())
