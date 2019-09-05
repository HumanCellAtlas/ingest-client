import os
from functools import reduce

import jsonschema
import requests

from .errorreport import ErrorReport
from .validationreport import ValidationReport

BUNDLE_SCHEMA_BASE_URL = "https://schema.humancellatlas.org/bundle/%s/"
BUNDLE_SCHEMA_VERSION = "5.1.0"
BUNDLE_SCHEMA_VERSION = os.environ.get('BUNDLE_SCHEMA_VERSION', BUNDLE_SCHEMA_VERSION)
BUNDLE_SCHEMA_BASE_URL = os.environ.get('BUNDLE_SCHEMA_BASE_URL', BUNDLE_SCHEMA_BASE_URL % BUNDLE_SCHEMA_VERSION)


class BundleValidator:

    def validate(self, metadata, schema_type, version=BUNDLE_SCHEMA_VERSION):

        """
        given a json document(metadata) and a json-schema(schema), validates the
        schema and returns a ValidationReport
        """
        schema = self.load_bundle_schema(schema_type, version)

        validator = jsonschema.Draft4Validator(schema=schema)
        if validator.is_valid(instance=metadata):
            ValidationReport.validation_report_ok()
            return True
        else:
            validation_report = ValidationReport()
            validation_report.validation_state = "INVALID"

            for error in validator.iter_errors(instance=metadata):
                validation_report.error_reports.append(
                    ErrorReport(self.generate_error_message(error), error,
                                "schema validation"))

            return validation_report
            # return False

    def generate_error_message(self, error):
        """
        Given an error object, generates an error message
        :param error: a jsonschema ValidationError
        :return: error message string generated from the error
        """
        path_to_error_in_document = reduce((lambda key1, key2: str(key1) + "." + str(key2)),
                                           error.absolute_path) if len(error.absolute_path) > 0 else "root of document"
        return "Error: " + error.message + " at " + path_to_error_in_document

    def extract_schema_url_from_document(self, metadata_document):
        try:
            return metadata_document["core"]["schema_url"]
        except KeyError:
            raise ("Could not find schema_url")

    def get_schema_from_url(self, schema_url):
        return requests.get(schema_url).json()

    def load_bundle_schema(self, schema_type, version):

        url = BUNDLE_SCHEMA_BASE_URL + schema_type
        if version:
            url = "https://schema.humancellatlas.org/bundle/" + version + "/" + schema_type
        schema = self.get_schema_from_url(url)

        return schema
