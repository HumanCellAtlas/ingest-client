from unittest import TestCase

from mock import MagicMock, patch, call
from openpyxl import Workbook

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion.column_specification import ColumnSpecification
from ingest.importer.conversion.conversion_strategy import CellConversion
from ingest.importer.conversion.template_manager import TemplateManager, RowTemplate
from ingest.importer.data_node import DataNode


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
        tab_config = MagicMock(name='tab_config')
        tab_config.get_key_for_label = MagicMock(return_value='concrete_entity')

        schema_template = MagicMock(name='schema_template')
        schema_template.get_tabs_config = MagicMock(return_value=tab_config)

        schema_url = 'https://schema.humancellatlas.org/type/biomaterial/5.1.0/donor_organsim'

        lookup_map = {
            'concrete_entity': {
                'schema': {
                    'domain_entity': 'biomaterial',
                    'url': schema_url
                }
            }
        }

        schema_template.lookup = lambda key: lookup_map.get(key)

        ingest_api = MagicMock(name='ingest_api')

        # and:
        template_manager = TemplateManager(schema_template, ingest_api)

        # and:
        workbook = Workbook()
        donor_worksheet = workbook.create_sheet('Donor')

        # when:
        data_node:DataNode = template_manager.create_template_node(donor_worksheet)

        # then:
        data = data_node.as_dict()
        self.assertEqual(schema_url, data.get('describedBy'))
        self.assertEqual('biomaterial', data.get('schema_type'))

    @patch.object(ColumnSpecification, 'build_raw')
    @patch.object(conversion_strategy, 'determine_strategy')
    def test_create_row_template(self, determine_strategy, build_raw):
        # given:
        schema_template = MagicMock(name='schema_template')
        ingest_api = MagicMock(name='ingest_api')

        # and:
        tabs_config = MagicMock('tabs_config')
        object_type = 'sample_object'
        tabs_config.get_key_for_label = MagicMock(return_value=object_type)
        schema_template.get_tabs_config = MagicMock(return_value=tabs_config)

        # and: set up column spec
        name_column_spec = MagicMock('name_column_spec')
        numbers_column_spec = MagicMock('numbers_column_spec')
        build_raw.side_effect = [name_column_spec, numbers_column_spec]

        # and: set up raw spec
        name_raw_spec = MagicMock('name_raw_spec')
        name_raw_parent_spec = MagicMock('name_raw_parent_spec')
        numbers_raw_spec = MagicMock('numbers_raw_spec')

        # TODO move the logic of creating the column spec to SchemaTemplate
        # and:
        schema = {'schema': {'domain_entity': 'main_category/subdomain'}}
        spec_map = {
            'user.profile.first_name': name_raw_spec,
            'user.profile': name_raw_parent_spec,
            'numbers': numbers_raw_spec,
            'user': schema
        }
        schema_template.lookup = lambda key: spec_map.get(key, None)

        # and:
        name_strategy = MagicMock('name_strategy')
        numbers_strategy = MagicMock('numbers_strategy')
        determine_strategy.side_effect = [name_strategy, numbers_strategy]

        # and: prepare worksheet
        workbook = Workbook()
        worksheet = workbook.create_sheet('sample')
        worksheet['A4'] = 'user.profile.first_name'
        worksheet['B4'] = 'numbers'

        # when:
        template_manager = TemplateManager(schema_template, ingest_api)
        row_template: RowTemplate = template_manager.create_row_template(worksheet)

        # then:
        expected_calls = [
            call('user.profile.first_name', 'sample_object', 'main_category', name_raw_spec,
                 order_of_occurence=1, parent=name_raw_parent_spec),
            call('numbers', 'sample_object', None, numbers_raw_spec, order_of_occurence=1, parent=None)
        ]
        build_raw.assert_has_calls(expected_calls)
        determine_strategy.assert_has_calls([call(name_column_spec), call(numbers_column_spec)])

        # and:
        self.assertIsNotNone(row_template)
        self.assertEqual(2, len(row_template.cell_conversions))
        self.assertTrue(name_strategy in row_template.cell_conversions)
        self.assertTrue(numbers_strategy in row_template.cell_conversions)

    @patch.object(ColumnSpecification, 'build_raw')
    @patch.object(conversion_strategy, 'determine_strategy')
    def test_create_row_template_with_default_values(self, determine_strategy, build_raw):
        # given:
        schema_template = MagicMock('schema_template')
        ingest_api = MagicMock(name='ingest_api')

        # and:
        schema_url = 'http://schema.sample.com/profile'
        self._mock_schema_lookup(schema_template, schema_url=schema_url, main_category='profile',
                                 object_type='profile_type')

        # and:
        build_raw.return_value = MagicMock('column_spec')
        determine_strategy.return_value = FakeConversion('')

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('profile')
        worksheet['A4'] = 'profile.name'

        # when:
        template_manager = TemplateManager(schema_template, ingest_api)
        template_manager.get_schema_url = MagicMock(return_value=schema_url)
        row_template = template_manager.create_row_template(worksheet)

        # then:
        content_defaults = row_template.default_values
        self.assertIsNotNone(content_defaults)
        self.assertEqual(schema_url, content_defaults.get('describedBy'))
        self.assertEqual('profile', content_defaults.get('schema_type'))

    @patch.object(conversion_strategy, 'determine_strategy')
    def test_create_row_template_with_none_header(self, determine_strategy):
        # given:
        schema_template = MagicMock('schema_template')
        ingest_api = MagicMock(name='ingest_api')

        # and:
        do_nothing_strategy = FakeConversion('')
        determine_strategy.return_value = do_nothing_strategy

        # and:
        self._mock_schema_lookup(schema_template)

        # and:
        workbook = Workbook()
        worksheet = workbook.create_sheet('sample')
        worksheet['A4'] = None

        # when:
        template_manager = TemplateManager(schema_template, ingest_api)
        row_template = template_manager.create_row_template(worksheet)

        # then:
        self.assertEqual(0, len(row_template.cell_conversions))

    @staticmethod
    def _mock_schema_lookup(schema_template, schema_url='', object_type='', main_category=None):
        tabs_config = MagicMock('tabs_config')
        tabs_config.get_key_for_label = MagicMock(return_value=object_type)
        schema_template.get_tabs_config = MagicMock(return_value=tabs_config)
        schema_template.get_latest_schema = MagicMock(return_value=schema_url)

        domain_entity = f'{main_category}/{object_type}' if main_category else object_type
        schema = {'schema': {
            'domain_entity': domain_entity,
            'url': schema_url
        }}
        spec_map = {
            object_type: schema
        }
        schema_template.lookup = lambda key: spec_map.get(key)

    def test_get_schema_type(self):
        # given
        schema_template = MagicMock(name='schema_template')
        ingest_api = MagicMock(name='ingest_api')

        spec = {
            'schema': {
                'high_level_entity': 'type',
                'domain_entity': 'biomaterial',
                'module': 'donor_organism',
                'url': 'https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism'
            }
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template, ingest_api)

        # when:
        domain_entity = template_manager.get_domain_entity('cell_suspension')

        # then:
        self.assertEqual('biomaterial', domain_entity)

    def test_get_schema_url(self):
        # given
        schema_template = MagicMock(name='schema_template')
        ingest_api = MagicMock(name='ingest_api')
        latest_url = 'https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism'

        spec = {
            'schema': {
                'high_level_entity': 'type',
                'domain_entity': 'biomaterial',
                'module': 'donor_organism',
                'url': latest_url
            }
        }

        schema_template.lookup = MagicMock(name='lookup', return_value=spec)
        template_manager = TemplateManager(schema_template, ingest_api)
        template_manager.get_latest_schema_url = MagicMock(return_value=latest_url)

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

        ingest_api = MagicMock(name='ingest_api')

        template_manager = TemplateManager(schema_template, ingest_api)

        # when:
        entity = template_manager.get_concrete_entity_of_tab('Specimen from organism')

        # then:
        self.assertEqual('specimen_from_organism', entity)


class FakeConversion(CellConversion):

    def __init__(self, field):
        self.field = field

    def apply(self, metadata, cell_data):
        metadata.define_content(self.field, cell_data)


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
        self.assertEqual('Juan', result.get_content('first_name'))
        self.assertEqual('dela Cruz', result.get_content('last_name'))

        # and:
        address = result.get_content('address')
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
        self.assertEqual(schema_url, result.get_content('describedBy'))
        self.assertEqual('extra field', result.get_content('extra_field'))
