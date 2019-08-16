from unittest import TestCase

from mock import MagicMock

import ingest
from ingest.api.ingestapi import IngestApi
from ingest.importer.importer import XlsImporter

import ingest.importer.submission

ERROR_TEMPLATE = {
    'type': 'http://importer.ingest.data.humancellatlas.org/Error',
    'title': 'An error during submission occurred.',
}


class IngestXlsImporterTest(TestCase):
    def setUp(self):
        # Setup mocked APIs
        self.mock_ingest_api = MagicMock(spec=IngestApi)

    def test_import_file_error(self):
        # given:
        importer = XlsImporter(ingest_api=self.mock_ingest_api)
        error = ingest.importer.submission.Error(
            'http://unittest.importer.ingest.data.humancellatlas.org/Error',
            'Error thrown for Unit Test')
        error_json = ERROR_TEMPLATE.copy()
        error_json['detail'] = str(error)
        importer._generate_spreadsheet_json = MagicMock(side_effect=error)
        importer.logger.error = MagicMock()

        # when:
        importer.import_file(file_path=None, submission_url=None, project_uuid=None)

        # then:
        self.mock_ingest_api.create_submission_error.assert_called_once_with(None, error_json)

    def test_import_file_exception(self):
        # given:
        importer = XlsImporter(ingest_api=self.mock_ingest_api)
        exception = Exception('Error thrown for Unit Test')
        exception_json = ERROR_TEMPLATE.copy()
        exception_json['detail'] = str(exception)
        importer._generate_spreadsheet_json = MagicMock(side_effect=exception)
        importer.logger.error = MagicMock()

        # when:
        importer.import_file(file_path=None, submission_url=None, project_uuid=None)

        # then:
        self.mock_ingest_api.create_submission_error.assert_called_once_with(None, exception_json)
