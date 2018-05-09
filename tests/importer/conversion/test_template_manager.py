from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion.data_converter import Converter, ListConverter, DataType, \
    IntegerConverter, BooleanConverter
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.template.schematemplate import SchemaTemplate


def _mock_schema_template_lookup(value_type='string', multivalue=False):
    schema_template = SchemaTemplate()
    single_string_spec = {
        'value_type': value_type,
        'multivalue': multivalue
    }
    schema_template.lookup = MagicMock(name='lookup', return_value=single_string_spec)
    return schema_template


# TODO make the processing of value type case insensitive
class TemplateManagerTest(TestCase):

    def test_get_converter_for_string(self):
        # given:
        schema_template = _mock_schema_template_lookup()
        template_manager = TemplateManager(schema_template)

        # when:
        header_name = 'path.to.field.project_shortname'
        converter = template_manager.get_converter(header_name)

        # then:
        schema_template.lookup.assert_called_with(header_name)
        self.assertIsInstance(converter, Converter)

    def test_get_converter_for_string_array(self):
        # given:
        schema_template = _mock_schema_template_lookup(multivalue=True)
        template_manager = TemplateManager(schema_template)

        # when:
        header_name = 'path.to.field.names'
        converter = template_manager.get_converter(header_name)

        # then:
        schema_template.lookup.assert_called_with(header_name)
        self.assertIsInstance(converter, ListConverter)
        self.assertEqual(DataType.STRING, converter.base_type)

    def test_get_converter_for_integer(self):
        # given:
        schema_template = _mock_schema_template_lookup(value_type='integer')
        template_manager = TemplateManager(schema_template)

        # when:
        header_name = 'path.to.field'
        converter = template_manager.get_converter(header_name)

        # then:
        self.assertIsInstance(converter, IntegerConverter)

    def test_get_converter_for_integer_array(self):
        # given:
        schema_template = _mock_schema_template_lookup(value_type='integer', multivalue=True)
        template_manager = TemplateManager(schema_template)

        # when:
        converter = template_manager.get_converter('path.to.field')

        # then:
        self.assertIsInstance(converter, ListConverter)
        self.assertEqual(DataType.INTEGER, converter.base_type)

    def test_get_converter_for_boolean(self):
        # given:
        schema_template = _mock_schema_template_lookup(value_type='boolean')
        template_manager = TemplateManager(schema_template)

        # when:
        converter = template_manager.get_converter('path.to.field')

        # then:
        self.assertIsInstance(converter, BooleanConverter)
