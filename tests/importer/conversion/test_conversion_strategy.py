from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion.conversion_strategy import DirectCellConversion, \
    ListElementCellConversion, CellConversion, IdentityCellConversion, LinkedIdentityCellConversion
from ingest.importer.conversion.column_specification import ColumnSpecification
from ingest.importer.conversion.data_converter import DataType
from ingest.importer.data_node import DataNode


def _mock_column_spec(field_name='field_name', data_type=DataType.STRING, multivalue=False,
                      field_of_list_member=False, converter=None):
    column_spec: ColumnSpecification = MagicMock('column_spec')
    column_spec.field_name = field_name
    column_spec.determine_converter = lambda: converter
    column_spec.is_field_of_list_element = lambda: field_of_list_member
    return column_spec


class ModuleTest(TestCase):

    def test_determine_strategy_for_direct_conversion(self):
        # given:
        converter = MagicMock('converter')
        column_spec = _mock_column_spec(field_name='user.first_name', converter=converter)

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, DirectCellConversion)
        self.assertEqual('user.first_name', strategy.field)
        self.assertEqual(converter, strategy.converter)

    def test_determine_strategy_for_field_of_list_element(self):
        # given:
        converter = MagicMock('converter')
        column_spec = _mock_column_spec(field_name='member.field.list', converter=converter,
                                        field_of_list_member=True)

        # when:
        strategy:CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, ListElementCellConversion)
        self.assertEqual('member.field.list', strategy.field)
        self.assertEqual(converter, strategy.converter)


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


class IdentityCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = IdentityCellConversion('product_id', converter)

        # and:
        data_node = DataNode()

        # when:
        cell_conversion.apply(data_node, 'product_no_144')

        # then:
        self.assertEqual(data_node['_object_id'], 'product_no_144 - converted')


class LinkedIdentityCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = LinkedIdentityCellConversion('items', converter)

        # and:
        data_node = DataNode()

        # when:
        cell_conversion.apply(data_node, 'item_no_29')

        # then:
        links = data_node['_links']
        self.assertIsNotNone(links)

        # and:
        items = links.get('items')
        self.assertEqual(1, len(items))
        self.assertTrue('item_no_29 - converted' in items)
