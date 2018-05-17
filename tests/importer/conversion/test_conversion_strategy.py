from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion.conversion_strategy import DirectCellConversion, \
    ListElementCellConversion, ColumnSpecification
from ingest.importer.conversion.data_converter import DataType
from ingest.importer.data_node import DataNode


class ModuleTest(TestCase):

    def test_determine_strategy_for_string(self):
        # given:
        string_column_spec:ColumnSpecification = MagicMock('column_spec')
        string_column_spec.data_type = DataType.STRING
        string_column_spec.is_multivalue = lambda: False

        # when:
        strategy = conversion_strategy.determine_strategy(string_column_spec)

        # then:
        self.assertIsNotNone(strategy)


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


class DirectCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        int_converter = MagicMock('int_converter')
        int_converter.convert = lambda __: 27

        # and:
        cell_conversion = DirectCellConversion('user.age', int_converter)

        # when:
        data_node = DataNode()
        cell_conversion.apply(data_node, '27')

        # then:
        user = data_node.as_dict().get('user')
        self.assertIsNotNone(user)
        self.assertEqual(27, user.get('age'))


def _create_mock_string_converter():
    converter = MagicMock('converter')
    converter.convert = lambda data: f'{data} - converted'
    return converter


class ListElementCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = ListElementCellConversion('list_of_things.name', converter)

        # when:
        data_node = DataNode()
        cell_conversion.apply(data_node, 'sample')

        # then:
        list_of_things = data_node.as_dict().get('list_of_things')
        self.assertIsNotNone(list_of_things)
        self.assertEqual(1, len(list_of_things))

        # and:
        thing = list_of_things[0]
        self.assertEqual('sample - converted', thing.get('name'))

    def test_apply_previously_processed_field(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = ListElementCellConversion('user.basket.product_name', converter)

        # and:
        data_node = DataNode()
        data_node['user.basket'] = [{'quantity': 3}]

        # when:
        cell_conversion.apply(data_node, 'apple')

        # then:
        basket = data_node.as_dict().get('user').get('basket')
        self.assertEqual(1, len(basket))

        # and:
        current_element = basket[0]
        self.assertEqual(3, current_element.get('quantity'))
        self.assertEqual('apple - converted', current_element.get('product_name'))
