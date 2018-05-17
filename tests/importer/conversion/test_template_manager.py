from unittest import TestCase

from mock import MagicMock
from openpyxl import Workbook

from ingest.importer.conversion.conversion_strategy import CellConversion, DirectCellConversion
from ingest.importer.conversion.data_converter import Converter, ListConverter, DataType, \
    IntegerConverter, BooleanConverter
from ingest.importer.conversion.template_manager import TemplateManager, RowTemplate, \
    ColumnSpecification
from ingest.importer.data_node import DataNode
from ingest.template.schema_template import SchemaTemplate


def _mock_schema_template_lookup(value_type='string', multivalue=False):
    schema_template = MagicMock(name='schema_template')
    single_string_spec = {
        'value_type': value_type,
        'multivalue': multivalue
    }
    schema_template.lookup = MagicMock(name='lookup', return_value=single_string_spec)
    return schema_template


class TemplateManagerTest(TestCase):

    def test_create_template_node(self):
        # given:
        schema_template = MagicMock(name='schema_template')
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
        self.assertEqual('biomaterial', data.get('schema_type'))

    def test_create_row_template(self):
        # given:
        schema_template = MagicMock(name='schema_template')
        column_spec = {
            'value_type': 'string',
            'multivalue': False
        }
        schema_template.get_key_for_label = lambda: column_spec

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('sample')
        worksheet['A4'] = 'user.profile.first_name'

        # and:
        template_manager = TemplateManager(schema_template)

        # when:
        row_template:RowTemplate = template_manager.create_row_template(worksheet)

        # then:
        self.assertIsNotNone(row_template)
        self.assertEqual(1, len(row_template.cell_conversions))

        # and:
        conversion:CellConversion = row_template.cell_conversions[0]
        self.assertIsInstance(conversion, DirectCellConversion)
        self.assertEqual('user.profile.first_name', conversion.field)
        self.assertIsInstance(conversion.converter, Converter)

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

        schema_template = MagicMock(name='schema_template')
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

        schema_template = MagicMock(name='schema_template')
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
        schema_template = MagicMock(name='schema_template')
        schema_template.lookup = MagicMock(name='lookup', return_value=None)
        template_manager = TemplateManager(schema_template)

        # when:
        is_parent_multivalue = template_manager.is_parent_field_multivalue('path.object_list_field.subfield')

        # then:
        self.assertFalse(is_parent_multivalue)

    def test_get_schema_type(self):
        # given
        schema_template = MagicMock(name='schema_template')
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
        domain_entity = template_manager.get_domain_entity('cell_suspension')

        # then:
        self.assertEqual('biomaterial', domain_entity)

    def test_get_schema_url(self):
        # given
        schema_template = MagicMock(name='schema_template')
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
        url = template_manager.get_schema_url('cell_suspension')

        # then:
        self.assertEqual('https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism', url)

    def test_get_entity_of_tab(self):
        # given

        key_label_map = {
            'Project': 'project',
            'Donor': 'donor',
            'Specimen from organism': 'specimen_from_organism'
        }

        fake_tabs_config = MagicMock(name='tabs_config')
        fake_tabs_config.get_key_for_label = lambda key: key_label_map.get(key)

        schema_template = MagicMock(name='schema_template')
        schema_template.get_tabs_config = MagicMock(return_value=fake_tabs_config)

        template_manager = TemplateManager(schema_template)

        # when:
        entity = template_manager.get_concrete_entity_of_tab('Specimen from organism')

        # then:
        self.assertEqual('specimen_from_organism', entity)

    def test_is_identifier_field_true(self):
        # given
        spec = {
            'multivalue': False,
            'required': True,
            'user_friendly': 'Biomaterial ID',
            'description': 'A unique ID for this biomaterial.',
            'example': None,
            'value_type': 'string',
            'identifiable': True
        }

        schema_template = MagicMock(name='schema_template')
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)

        template_manager = TemplateManager(schema_template)

        # when:
        is_id = template_manager.is_identifier_field('header_name')

        # then:
        self.assertTrue(is_id)

    def test_is_identifier_field_false(self):
        # given
        spec = {
            'user_friendly': 'Biomaterial Name',
        }

        schema_template = MagicMock(name='schema_template')
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)

        template_manager = TemplateManager(schema_template)

        # when:
        is_id = template_manager.is_identifier_field('header_name')

        # then:
        self.assertFalse(is_id)

    def test_get_concrete_entity_of_column(self):
        schema_template = MagicMock(name='schema_template')
        template_manager = TemplateManager(schema_template)

        concrete_entity = template_manager.get_concrete_entity_of_column('cell_suspension.id_column')
        self.assertEqual('cell_suspension', concrete_entity)

        concrete_entity = template_manager.get_concrete_entity_of_column('cell_suspension.id_column.any_column')
        self.assertEqual('cell_suspension', concrete_entity)

        concrete_entity = template_manager.get_concrete_entity_of_column('donor.id_column')
        self.assertEqual('donor', concrete_entity)


class FakeConversion(CellConversion):

    def __init__(self, field):
        self.field = field

    def apply(self, data_node, cell_data):
        data_node[self.field] = cell_data


class RowTemplateTest(TestCase):

    def test_do_import(self):
        # given:
        cell_conversions = [FakeConversion('first_name'), FakeConversion('last_name'),
                            FakeConversion('address.city'), FakeConversion('address.country')]
        row_template = RowTemplate(cell_conversions)

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('profile')
        worksheet['A1'] = 'Juan'
        worksheet['B1'] = 'dela Cruz'
        worksheet['C1'] = 'Manila'
        worksheet['D1'] = 'Philippines'
        row = list(worksheet.rows)[0]

        # when:
        result = row_template.do_import(row)

        # then:
        self.assertIsNotNone(result)
        self.assertEqual('Juan', result.get('first_name'))
        self.assertEqual('dela Cruz', result.get('last_name'))

        # and:
        address = result.get('address')
        self.assertIsNotNone(address)
        self.assertEqual('Manila', address.get('city'))
        self.assertEqual('Philippines', address.get('country'))

    def test_do_import_with_default_values(self):
        # given:
        schema_url = 'https://sample.com/list_of_things'
        default_values = {
            'describedBy': schema_url,
            'extra_field': 'extra field'
        }

        # and:
        cell_conversions = [FakeConversion('name'), FakeConversion('description')]
        row_template = RowTemplate(cell_conversions, default_values=default_values)

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('list_of_things')
        worksheet['A1'] = 'pen'
        worksheet['B1'] = 'a thing used for writing'
        row = list(worksheet.rows)[0]

        # when:
        result = row_template.do_import(row)

        # then:
        self.assertEqual(schema_url, result.get('describedBy'))
        self.assertEqual('extra field', result.get('extra_field'))


class ColumnSpecificationTest(TestCase):

    def test_construct_from_raw_spec(self):
        # given:
        raw_string_spec = {
            'value_type': 'string',
            'multivalue': False
        }

        # and:
        raw_int_array_spec = {
            'value_type': 'integer',
            'multivalue': True
        }

        # when:
        string_column_spec = ColumnSpecification(raw_string_spec)
        int_array_column_spec = ColumnSpecification(raw_int_array_spec)

        # then:
        self.assertEqual(DataType.STRING, string_column_spec.data_type)
        self.assertFalse(string_column_spec.is_multivalue())

        # and:
        self.assertEqual(DataType.INTEGER, int_array_column_spec.data_type)
        self.assertTrue(int_array_column_spec.is_multivalue())

    def test_construct_from_raw_spec_with_parent_spec(self):
        # given:
        raw_spec = {
            'value_type': 'boolean',
            'multivalue': True
        }

        # and:
        raw_single_value_parent_spec = {
            'multivalue': False
        }

        # and:
        raw_multi_value_parent_spec = {
            'multivalue': True
        }

        # when:
        single_column_spec = ColumnSpecification(raw_spec, parent=raw_single_value_parent_spec)
        multi_column_spec = ColumnSpecification(raw_spec, parent=raw_multi_value_parent_spec)

        # then:
        self.assertFalse(single_column_spec.is_field_of_list_member())
        self.assertTrue(multi_column_spec.is_field_of_list_member())
