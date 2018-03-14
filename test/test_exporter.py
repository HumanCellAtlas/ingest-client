from unittest import TestCase

from broker.ingestexportservice import IngestExporter


class TestExporter(TestCase):

    def test_processProjectBundle(self):
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
        bundle = exporter.getBundleDocument(project_entity)

        # then:
        self.assertEqual(bundle['hca_ingest']['submissionDate'], project_entity['submissionDate'])
