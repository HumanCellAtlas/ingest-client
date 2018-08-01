from unittest import TestCase

import copy

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
