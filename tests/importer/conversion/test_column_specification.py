from unittest import TestCase

import copy

from mock import MagicMock

from ingest.importer.conversion import column_specification
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import DataType, IntegerConverter, \
    BooleanConverter, ListConverter, StringConverter, DefaultConverter


class ColumnSpecificationTest(TestCase):

    def test_build_raw_single_type(self):
        # given:
        raw_string_spec = {
            'value_type': 'string',
            'multivalue': False,
            'identifiable': True
        }

        # when:
        string_column_spec = ColumnSpecification.build_raw('user.name', 'user', 'profile_entry',
                                                           raw_string_spec)

        # then:
        self.assertEqual('user.name', string_column_spec.field_name)
        self.assertEqual('profile_entry', string_column_spec.main_category)
        self.assertEqual(DataType.STRING, string_column_spec.data_type)
        self.assertFalse(string_column_spec.is_multivalue())
        self.assertTrue(string_column_spec.is_identity())

    def test_build_raw_multivalue(self):
        # given:
        raw_int_array_spec = {
            'value_type': 'integer',
            'multivalue': True
        }

        # when:
        int_array_column_spec = ColumnSpecification.build_raw('sample.numbers', 'user',
                                                              'profile_entry', raw_int_array_spec)

        # then:
        self.assertEqual('sample.numbers', int_array_column_spec.field_name)
        self.assertEqual('profile_entry', int_array_column_spec.main_category)
        self.assertEqual(DataType.INTEGER, int_array_column_spec.data_type)
        self.assertTrue(int_array_column_spec.is_multivalue())
        self.assertFalse(int_array_column_spec.is_identity())

    def test_build_raw_external_reference(self):
        # given:
        external_raw_spec = {
            # 'value_type': 'string',
            'identifiable': True,
            'external_reference': True
        }

        # and:
        non_external_raw_spec = copy.deepcopy(external_raw_spec)
        non_external_raw_spec['external_reference'] = False

        # when:
        external_spec = ColumnSpecification.build_raw('profile.uuid', 'profile', 'personal_info',
                                                      external_raw_spec)
        non_external_spec = ColumnSpecification.build_raw('user.id', 'user', 'personal_info',
                                                          non_external_raw_spec)

        # then:
        self.assertEqual('profile.uuid', external_spec.field_name)
        self.assertEqual('profile', external_spec.object_type)
        self.assertEqual('personal_info', external_spec.main_category)
        # self.assertEqual(DataType.STRING, external_spec.data_type)
        self.assertTrue(external_spec.is_external_reference())

        # then:
        self.assertEqual('user.id', non_external_spec.field_name)
        self.assertFalse(non_external_spec.is_external_reference())

    def test_build_raw_spec_with_parent_spec(self):
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
        single_column_spec = ColumnSpecification.build_raw('', '', '', raw_spec,
                                                           parent=raw_single_value_parent_spec)
        multi_column_spec = ColumnSpecification.build_raw('', '', '', raw_spec,
                                                          parent=raw_multi_value_parent_spec)

        # then:
        self.assertFalse(single_column_spec.is_field_of_list_element())
        self.assertTrue(multi_column_spec.is_field_of_list_element())

    def test_look_up_id_field(self):
        # given:
        schema_spec = {
            'product.id': {'value_type': 'integer', 'multivalue': False, 'identifiable': True}
        }
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)
        # when:
        id_spec = column_specification.look_up(schema_template, 'product.id')

        # then:
        self.assertTrue(id_spec.is_identity())
        self.assertEqual('product.id', id_spec.field_name)
        self.assertEqual('integer', id_spec.data_type)

    def test_look_up_object_field(self):
        # given:
        schema_spec = {'product.name': {'value_type': 'string', 'multivalue': False}}
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        name_spec = column_specification.look_up(schema_template, 'product.name')

        # then:
        self.assertFalse(name_spec.multivalue)
        self.assertEqual('product.name', name_spec.field_name)
        self.assertEqual('product', name_spec.object_type)
        self.assertEqual('merchandise', name_spec.main_category)
        self.assertEqual('string', name_spec.data_type)

    def test_look_up_multivalue_object_field(self):
        # given:
        schema_spec = {'product.remarks': {'value_type': 'string', 'multivalue': True}}
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        remarks_spec = column_specification.look_up(schema_template, 'product.remarks')

        # then:
        self.assertTrue(remarks_spec.multivalue)
        self.assertEqual('product.remarks', remarks_spec.field_name)

    def test_look_up_nested_object_field(self):
        # given:
        schema_spec = {
            'product.reviews.rating': {'value_type': 'integer','multivalue': False},
            'product.reviews': {'value_type': 'object', 'multivalue': True}
        }
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        review_rating_spec = column_specification.look_up(schema_template, 'product.reviews.rating')

        # then:
        self.assertFalse(review_rating_spec.multivalue)
        self.assertTrue(review_rating_spec.multivalue_parent)

    def test_look_up_external_id(self):
        # given:
        schema_spec = {
            'product.manufacturer_id': {
                'value_type': 'integer',
                'multivalue': False,
                'identifiable': True,
                'external_reference': True
            }
        }
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        manufacturer_id_spec = column_specification.look_up(schema_template,
                                                            'product.manufacturer_id')

        # then:
        self.assertTrue(manufacturer_id_spec.identity)
        self.assertTrue(manufacturer_id_spec.external_reference)
        self.assertEqual('product.manufacturer_id', manufacturer_id_spec.field_name)
        self.assertEqual('integer', manufacturer_id_spec.data_type)

    @staticmethod
    def _prepare_mock_schema_template(domain_type, domain_entity=None, schema_spec_map=None):
        value_map = copy.deepcopy(schema_spec_map)
        if domain_entity:
            type_spec = {
                'schema': {'domain_entity': domain_entity}
            }
            value_map.update({domain_type: type_spec})

        schema_template = MagicMock(name='schema_template')
        schema_template.lookup = lambda key: value_map[key]
        return schema_template

    def test_determine_converter_for_single_value(self):
        # expect:
        self._assert_correct_converter_single_value(DataType.STRING, StringConverter)
        self._assert_correct_converter_single_value(DataType.INTEGER, IntegerConverter)
        self._assert_correct_converter_single_value(DataType.BOOLEAN, BooleanConverter)
        self._assert_correct_converter_single_value(DataType.UNDEFINED, DefaultConverter)

    def _assert_correct_converter_single_value(self, data_type: DataType, expected_converter_type):
        # given:
        column_spec = ColumnSpecification('field', 'object_type', 'main_category', data_type)

        # when:
        converter = column_spec.determine_converter()

        # then:
        self.assertIsInstance(converter, expected_converter_type)

    def test_determine_converter_for_multivalue_type(self):
        # expect:
        self._assert_correct_converter_multivalue(DataType.STRING)
        self._assert_correct_converter_multivalue(DataType.INTEGER)
        self._assert_correct_converter_multivalue(DataType.BOOLEAN)
        self._assert_correct_converter_multivalue(DataType.UNDEFINED)

    def _assert_correct_converter_multivalue(self, data_type):
        # given:
        column_spec = ColumnSpecification('field', 'object_type', 'main_category',
                                          data_type, multivalue=True)

        # when:
        converter = column_spec.determine_converter()

        # then:
        self.assertIsInstance(converter, ListConverter)
        self.assertEqual(data_type, converter.base_type)

    def test_get_conversion_type_member_field(self):
        # given:
        column_spec = ColumnSpecification('user.name', 'user', 'user_data', DataType.STRING)

        # expect:
        self.assertEqual(ConversionType.MEMBER_FIELD, column_spec.get_conversion_type())

    def test_get_conversion_type_field_of_list_element(self):
        # given:
        column_spec = ColumnSpecification('product.product_name', 'product', 'store_entity',
                                          DataType.STRING, multivalue_parent=True)

        # expect:
        self.assertEqual(ConversionType.FIELD_OF_LIST_ELEMENT, column_spec.get_conversion_type())

    def test_get_conversion_type_identity(self):
        # given:
        column_spec = ColumnSpecification('product.product_id', 'product', 'store_entry',
                                          DataType.STRING, identity=True)

        # expect:
        self.assertEqual(ConversionType.IDENTITY, column_spec.get_conversion_type())

    def test_get_conversion_type_linked_identity(self):
        # given:
        column_spec = ColumnSpecification('account.number', 'user', 'profile_type',
                                          DataType.STRING, identity=True)

        # expect:
        self.assertEqual(ConversionType.LINKED_IDENTITY, column_spec.get_conversion_type())

    def test_get_conversion_type_linked_identity_order(self):
        # given:
        column_spec = ColumnSpecification('account.number', 'account', 'profile_type',
                                          DataType.STRING, identity=True, order_of_occurence=2)

        # expect:
        self.assertEqual(ConversionType.LINKED_IDENTITY, column_spec.get_conversion_type())

    def test_get_conversion_type_external_reference_identity(self):
        # given:
        column_spec = ColumnSpecification('account.uuid', 'user', 'profile_type',
                                          DataType.STRING, identity=True, external_reference=True)

        # expect:
        self.assertEqual(ConversionType.EXTERNAL_REFERENCE, column_spec.get_conversion_type())

    def test_get_conversion_type_linking_detail(self):
        # given:
        column_spec = ColumnSpecification('item.description', 'record', 'invoice_detail',
                                          DataType.STRING)

        # expect:
        self.assertEqual(ConversionType.LINKING_DETAIL, column_spec.get_conversion_type())
