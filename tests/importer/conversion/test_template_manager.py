from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion.data_converter import Converter, ListConverter, DataType
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.template.schematemplate import SchemaTemplate


class TemplateManagerTest(TestCase):

    def test_get_converter_for_string(self):
        # given:
        schema_template = SchemaTemplate()
        single_string_spec = {
            'value_type': 'string',
            'multivalue': False
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=single_string_spec)

        # and:
        template_manager = TemplateManager(schema_template)

        # when:
        header_name = 'path.to.field.project_shortname'
        converter = template_manager.get_converter(header_name)

        # then:
        schema_template.lookup.assert_called_with(header_name)
        self.assertIsInstance(converter, Converter)

    def test_get_converter_for_string_array(self):
        # given:
        schema_template = SchemaTemplate()
        string_array_spec = {
            'value_type': 'string',
            'multivalue': True
        }
        schema_template.lookup = MagicMock(return_value=string_array_spec)

        # and:
        template_manager = TemplateManager(schema_template)

        # when:
        header_name = 'path.to.field.names'
        converter = template_manager.get_converter(header_name)

        # then:
        schema_template.lookup.assert_called_with(header_name)
        self.assertIsInstance(converter, ListConverter)
        self.assertEqual(DataType.STRING, converter.base_type)
