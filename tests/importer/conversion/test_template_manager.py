from unittest import TestCase

from mock import MagicMock
from openpyxl import Workbook

from ingest.importer.conversion.data_converter import Converter, ListConverter, DataType, \
    IntegerConverter, BooleanConverter
from ingest.importer.conversion.template_manager import TemplateManager
from ingest.importer.data_node import DataNode
from ingest.template.schema_template import SchemaTemplate


def _mock_schema_template_lookup(value_type='string', multivalue=False):
    schema_template = SchemaTemplate([])
    single_string_spec = {
        'value_type': value_type,
        'multivalue': multivalue
    }
    schema_template.lookup = MagicMock(name='lookup', return_value=single_string_spec)
    return schema_template


class TemplateManagerTest(TestCase):

    def test_create_template_node(self):
        # given:
        schema_template = SchemaTemplate([])
        schema_url = 'https://schema.humancellatlas.org/type/biomaterial/5.1.0/donor_organsim'
        tab_spec = {
            'schema': {
                'domain_entity': 'biomaterial',
                'url': schema_url
            }
        }
        # TODO define get_tab_spec in SchemaTemplate
        schema_template.get_tab_spec = lambda title: tab_spec if title == 'Donor' else None

        # and:
        template_manager = TemplateManager(schema_template)

        # and:
        workbook = Workbook()
        donor_worksheet = workbook.create_sheet('Donor')

        # when:
        data_node:DataNode = template_manager.create_template_node(donor_worksheet)

        # then:
        data = data_node.as_dict()
        self.assertEqual(schema_url, data.get('describedBy'))


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

    def test_get_converter_for_boolean_array(self):
        # given:
        schema_template = _mock_schema_template_lookup(value_type='boolean', multivalue=True)
        template_manager = TemplateManager(schema_template)

        # when:
        converter = template_manager.get_converter('path.to.field')

        # then:
        self.assertIsInstance(converter, ListConverter)
        self.assertEqual(DataType.BOOLEAN, converter.base_type)

    def test_is_parent_field_multivalue_true(self):
        # given:

        schema_template = SchemaTemplate([])
        spec = {
            'multivalue': True,
            'value_type': 'object'
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template)

        # when:
        is_parent_multivalue = template_manager.is_parent_field_multivalue('path.object_list_field.subfield')

        # then:
        self.assertTrue(is_parent_multivalue)

    def test_is_parent_field_multivalue_false(self):
        # given:

        schema_template = SchemaTemplate([])
        spec = {
            'multivalue': False,
            'value_type': 'string'
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template)

        # when:
        is_parent_multivalue = template_manager.is_parent_field_multivalue('path.object_list_field.subfield')

        # then:
        self.assertFalse(is_parent_multivalue)

    def test_is_parent_field_multivalue_no_spec(self):
        # given:
        schema_template = SchemaTemplate([])
        schema_template.lookup = MagicMock(name='lookup', return_value=None)
        template_manager = TemplateManager(schema_template)

        # when:
        is_parent_multivalue = template_manager.is_parent_field_multivalue('path.object_list_field.subfield')

        # then:
        self.assertFalse(is_parent_multivalue)

    def test_get_schema_type(self):
        # given
        schema_template = SchemaTemplate([])
        spec = {
            'schema': {
                'high_level_entity': 'type',
                'domain_entity': 'biomaterial',
                'module': 'donor_organism',
                'url': 'https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism'
            }
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template)

        # when:
        domain_entity = template_manager.get_schema_type('cell_suspension')

        # then:
        self.assertEqual('biomaterial', domain_entity)


    def test_get_schema_url(self):
        # given
        schema_template = SchemaTemplate([])
        spec = {
            'schema': {
                'high_level_entity': 'type',
                'domain_entity': 'biomaterial',
                'module': 'donor_organism',
                'url': 'https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism'
            }
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template)

        # when:
        url = template_manager.get_schema_url('cell_suspension.path.field')

        # then:
        self.assertEqual('https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism', url)