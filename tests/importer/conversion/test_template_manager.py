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


class TemplateManagerTest(TestCase):

    def test_create_template_node(self):
        # given:
        schema_template = SchemaTemplate()

        # and:
        template_manager = TemplateManager(schema_template)

        # when:
        data_node = template_manager.create_template_node()

        # then:
        self.assertIsNotNone(data_node)


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

    def test_is_ontology_subfield_true(self):
        # given:

        schema_template = SchemaTemplate()
        spec = {
            'multivalue': True,
            'value_type': 'object',
            'schema': {
                'high_level_entity': 'module',
                'domain_entity': 'ontology',
                'module': 'species_ontology',
                'url': 'https://schema.humancellatlas.org/module/ontology/5.0.0/species_ontology'
            }
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template)

        # when:
        is_ontology = template_manager.is_ontology_subfield('path.ontology_field.subfield')

        # then:
        self.assertTrue(is_ontology)

    def test_is_ontology_subfield_false(self):
        # given:

        schema_template = SchemaTemplate()
        spec = {
            'multivalue': False,
            'value_type': 'string'
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template)

        # when:
        is_ontology = template_manager.is_ontology_subfield('path.ontology_field.subfield')

        # then:
        self.assertFalse(is_ontology)

    def test_is_ontology_subfield_false_no_spec(self):
        # given:

        schema_template = SchemaTemplate()
        schema_template.lookup = MagicMock(name='lookup', return_value=None)
        template_manager = TemplateManager(schema_template)

        # when:
        is_ontology = template_manager.is_ontology_subfield('path.ontology_field.subfield')

        # then:
        self.assertFalse(is_ontology)

    def test_get_schema_type(self):
        # given
        schema_template = SchemaTemplate()
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
        schema_template = SchemaTemplate()
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