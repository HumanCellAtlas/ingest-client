from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion.conversion_strategy import DirectCellConversion, \
    ListElementCellConversion, ColumnSpecification, CellConversion
from ingest.importer.conversion.data_converter import DataType, Converter, IntegerConverter, \
    BooleanConverter, ListConverter
from ingest.importer.data_node import DataNode


def _mock_column_spec(field_name='field_name', data_type=DataType.STRING, multivalue=False):
    column_spec: ColumnSpecification = MagicMock('column_spec')
    column_spec.field_name = field_name
    column_spec.data_type = data_type
    column_spec.is_multivalue = lambda: multivalue
    return column_spec


class ModuleTest(TestCase):

    def test_determine_strategy_for_string(self):
        # given:
        column_spec = _mock_column_spec(field_name='user.first_name')

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, DirectCellConversion)
        self.assertEqual('user.first_name', strategy.field)
        self.assertIsInstance(strategy.converter, Converter)

    def test_determine_strategy_for_integer(self):
        # given:
        column_spec = _mock_column_spec(field_name='item.count', data_type=
        DataType.INTEGER)

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, DirectCellConversion)
        self.assertEqual('item.count', strategy.field)
        self.assertIsInstance(strategy.converter, IntegerConverter)

    def test_determine_strategy_for_boolean(self):
        # given:
        column_spec = _mock_column_spec(field_name='alarm.repeating', data_type=DataType.BOOLEAN)

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, DirectCellConversion)
        self.assertEqual('alarm.repeating', strategy.field)
        self.assertIsInstance(strategy.converter, BooleanConverter)

    def test_determine_strategy_for_multivalue_types(self):
        # expect:
        self._assert_correct_strategy_for_multivalue_type(DataType.STRING)
        self._assert_correct_strategy_for_multivalue_type(DataType.INTEGER)
        self._assert_correct_strategy_for_multivalue_type(DataType.BOOLEAN)

    def _assert_correct_strategy_for_multivalue_type(self, data_type:DataType):
        # given:
        column_spec = _mock_column_spec(field_name='items', data_type=data_type, multivalue=True)

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, DirectCellConversion)
        self.assertEqual('items', strategy.field)
        self.assertIsInstance(strategy.converter, ListConverter)
        self.assertEqual(data_type, strategy.converter.base_type)

    def test_determine_strategy_for_field_of_list_element(self):
        # given:
        column_spec = _mock_column_spec(field_name='member.field.list', data_type=DataType.INTEGER)
        column_spec.is_field_of_list_member = lambda: True

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, ListElementCellConversion)
        self.assertEqual('member.field.list', strategy.field)
        self.assertIsInstance(strategy.converter, IntegerConverter)


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
        string_column_spec = ColumnSpecification('user.name', raw_string_spec)
        int_array_column_spec = ColumnSpecification('numbers', raw_int_array_spec)

        # then:
        self.assertEqual('user.name', string_column_spec.field_name)
        self.assertEqual(DataType.STRING, string_column_spec.data_type)
        self.assertFalse(string_column_spec.is_multivalue())

        # and:
        self.assertEqual('numbers', int_array_column_spec.field_name)
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
        single_column_spec = ColumnSpecification('', raw_spec, parent=raw_single_value_parent_spec)
        multi_column_spec = ColumnSpecification('', raw_spec, parent=raw_multi_value_parent_spec)

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
