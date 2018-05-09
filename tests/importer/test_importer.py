import json
import os
from os import listdir
from os.path import isfile, join
from unittest import TestCase

from openpyxl import Workbook

from ingest.importer.conversion.data_converter import Converter, ListConverter, BooleanConverter, \
    DataType
from ingest.importer.importer import WorksheetImporter
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.template.schematemplate import SchemaTemplate

BASE_PATH = os.path.dirname(__file__)


def load_json(file_path):
    # open JSON file and parse contents
    fh = open(file_path, 'r')
    data = json.load(fh)
    fh.close()

    return data


def empty_dir(directory):
    files = [f for f in listdir(directory) if isfile(join(directory, f))]

    for filename in files:
        os.unlink(join(directory, filename))


class WorksheetImporterTest(TestCase):

    def test_do_import(self):
        # given:
        worksheet_importer = WorksheetImporter()

        # and:
        boolean_converter = BooleanConverter()
        converter_mapping = {
            'projects.project.project_core.project_shortname': Converter(),
            'projects.project.miscellaneous': ListConverter(),
            'projects.project.numbers': ListConverter(data_type=DataType.INTEGER),
            'projects.project.is_active': boolean_converter,
            'projects.project.is_submitted': boolean_converter
        }

        # and:
        template_manager = TemplateManager(SchemaTemplate())
        template_manager.get_converter = lambda key: converter_mapping.get(key, Converter())
        ontology_fields_mapping = {
            'projects.project.genus_species.ontology': True,
            'projects.project.genus_species.text': True,
        }

        template_manager.is_ontology_subfield = (
            lambda field_name: ontology_fields_mapping.get(field_name)
        )

        template_manager.get_schema_url = (
            lambda: 'https://schema.humancellatlas.org/type/project/5.1.0/project'
        )

        template_manager.get_schema_type = (
            lambda: 'project'
        )

        # and:
        worksheet = self._create_test_worksheet()

        # when:
        json_list = worksheet_importer.do_import(worksheet, template_manager)

        # then:
        json = json_list[0]
        self.assertTrue(2, len(json_list))
        self.assertEqual('Tissue stability 2', json_list[1]['project_core']['project_shortname'])

        project_core = json['project_core']
        self.assertEqual('Tissue stability', project_core['project_shortname'])
        self.assertEqual('Ischaemic sensitivity of human tissue by single cell RNA seq.',
                         project_core['project_title'])

        # and:
        self.assertEqual(2, len(json['miscellaneous']))
        self.assertEqual(['extra', 'details'], json['miscellaneous'])

        # and:
        self.assertEqual(7, json['contributor_count'])

        # and
        self.assertEqual('Juan Dela Cruz||John Doe', json['contributors'])

        # and
        self.assertEqual([1, 2, 3], json['numbers'])

        # and
        self.assertEqual(True, json['is_active'])
        self.assertEqual(False, json['is_submitted'])

        # ontology field
        self.assertTrue(type(json['genus_species']) is list)
        self.assertEqual(1, len(json['genus_species']))
        self.assertEqual({'ontology': 'UO:000008', 'text': 'meter'}, json['genus_species'][0])

        self.assertEqual('https://schema.humancellatlas.org/type/project/5.1.0/project', json['describedBy'])
        self.assertEqual('project', json['schema_type'])

    def _create_test_worksheet(self):
        workbook = Workbook()
        worksheet = workbook.create_sheet('Project')
        worksheet['A1'] = 'projects.project.project_core.project_shortname'
        worksheet['A4'] = 'Tissue stability'
        worksheet['A5'] = 'Tissue stability 2'
        worksheet['B1'] = 'projects.project.project_core.project_title'
        worksheet['B4'] = 'Ischaemic sensitivity of human tissue by single cell RNA seq.'
        worksheet['C1'] = 'projects.project.miscellaneous'
        worksheet['C4'] = 'extra||details'
        worksheet['D1'] = 'projects.project.contributor_count'
        worksheet['D4'] = 7
        worksheet['E1'] = 'projects.project.contributors'
        worksheet['E4'] = 'Juan Dela Cruz||John Doe'
        worksheet['F1'] = 'projects.project.numbers'
        worksheet['F4'] = '1||2||3'
        worksheet['G1'] = 'projects.project.is_active'
        worksheet['G4'] = 'Yes'
        worksheet['H1'] = 'projects.project.is_submitted'
        worksheet['H4'] = 'No'
        worksheet['I1'] = 'projects.project.genus_species.ontology'
        worksheet['I4'] = 'UO:000008'
        worksheet['J1'] = 'projects.project.genus_species.text'
        worksheet['J4'] = 'meter'

        return worksheet
