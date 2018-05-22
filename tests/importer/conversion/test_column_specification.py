from unittest import TestCase

from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import DataType, Converter, IntegerConverter, \
    BooleanConverter, ListConverter


class ColumnSpecificationTest(TestCase):

    def test_build_raw(self):
        # given:
        raw_string_spec = {
            'value_type': 'string',
            'multivalue': False,
            'identifiable': True
        }

        # and:
        raw_int_array_spec = {
            'value_type': 'integer',
            'multivalue': True
        }

        # when:
        string_column_spec = ColumnSpecification.build_raw('user.name', raw_string_spec)
        int_array_column_spec = ColumnSpecification.build_raw('numbers', raw_int_array_spec)

        # then:
        self.assertEqual('user.name', string_column_spec.field_name)
        self.assertEqual(DataType.STRING, string_column_spec.data_type)
        self.assertFalse(string_column_spec.is_multivalue())
        self.assertTrue(string_column_spec.is_identity())

        # and:
        self.assertEqual('numbers', int_array_column_spec.field_name)
        self.assertEqual(DataType.INTEGER, int_array_column_spec.data_type)
        self.assertTrue(int_array_column_spec.is_multivalue())
        self.assertFalse(int_array_column_spec.is_identity())

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
        single_column_spec = ColumnSpecification.build_raw('', raw_spec,
                                                           parent=raw_single_value_parent_spec)
        multi_column_spec = ColumnSpecification.build_raw('', raw_spec,
                                                          parent=raw_multi_value_parent_spec)

        # then:
        self.assertFalse(single_column_spec.is_field_of_list_element())
        self.assertTrue(multi_column_spec.is_field_of_list_element())

    def test_determine_converter_for_single_value(self):
        # expect:
        self._assert_correct_converter_single_value(DataType.STRING, Converter)
        self._assert_correct_converter_single_value(DataType.INTEGER, IntegerConverter)
        self._assert_correct_converter_single_value(DataType.BOOLEAN, BooleanConverter)
        self._assert_correct_converter_single_value(DataType.UNDEFINED, Converter)

    def _assert_correct_converter_single_value(self, data_type:DataType, expected_converter_type):
        # given:
        column_spec = ColumnSpecification('field', data_type)

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
        column_spec = ColumnSpecification('field', data_type, multivalue=True)

        # when:
        converter = column_spec.determine_converter()

        # then:
        self.assertIsInstance(converter, ListConverter)
        self.assertEqual(data_type, converter.base_type)

    def test_get_conversion_type_member_field(self):
        # given:
        column_spec = ColumnSpecification('user.name', 'profile', DataType.STRING)

        # expect:
        self.assertEqual(ConversionType.MEMBER_FIELD, column_spec.get_conversion_type())

    def test_conversion_type_field_of_list_element(self):
        # given:
        column_spec = ColumnSpecification('product_name', 'store_schema', DataType.STRING,
                                          multivalue_parent=True)

        # expect:
        self.assertEqual(ConversionType.FIELD_OF_LIST_ELEMENT, column_spec.get_conversion_type())
