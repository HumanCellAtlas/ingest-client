from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion.conversion_strategy import DirectCellConversion, \
    ListElementCellConversion, CellConversion, IdentityCellConversion, LinkedIdentityCellConversion, \
    DoNothing
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import DataType, StringConverter
from ingest.importer.data_node import DataNode


def _mock_column_spec(field_name='field_name', converter=None,
                      conversion_type=ConversionType.UNDEFINED):
    column_spec: ColumnSpecification = MagicMock('column_spec')
    column_spec.field_name = field_name
    column_spec.determine_converter = lambda: converter
    column_spec.get_conversion_type = lambda: conversion_type
    return column_spec


class ModuleTest(TestCase):

    def test_determine_strategy_for_direct_conversion(self):
        # expect:
        self._assert_correct_strategy(ConversionType.MEMBER_FIELD, DirectCellConversion)

    def test_determine_strategy_for_field_of_list_element(self):
        # expect:
        self._assert_correct_strategy(ConversionType.FIELD_OF_LIST_ELEMENT,
                                      ListElementCellConversion)

    def test_determine_strategy_for_identity_field(self):
        # expect:
        self._assert_correct_strategy(ConversionType.IDENTITY, IdentityCellConversion)

    def test_determine_strategy_for_linked_identity_field(self):
        # expect:
        self._assert_correct_strategy(ConversionType.LINKED_IDENTITY, LinkedIdentityCellConversion)

    def _assert_correct_strategy(self, conversion_type, strategy_class):
        # given:
        converter = MagicMock('converter')
        column_spec = _mock_column_spec(field_name='product.product_id', converter=converter,
                                        conversion_type=conversion_type)

        # when:
        strategy: CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, strategy_class)
        self.assertEqual('product.product_id', strategy.field)
        self.assertEqual(converter, strategy.converter)

    def test_determine_strategy_for_unknown_type(self):
        # given:
        converter = MagicMock('converter')
        column_spec = _mock_column_spec(field_name='product.product_id', converter=converter,
                                        conversion_type=ConversionType.UNDEFINED)

        # when:
        undefined_strategy: CellConversion = conversion_strategy.determine_strategy(column_spec)
        none_strategy: CellConversion = conversion_strategy.determine_strategy(None)

        # then:
        self.assertIsInstance(undefined_strategy, DoNothing)
        self.assertIsInstance(none_strategy, DoNothing)


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
        content = data_node.as_dict().get(conversion_strategy.CONTENT_FIELD)
        self.assertIsNotNone(content)

        # and:
        user = content.get('user')
        self.assertIsNotNone(user)
        self.assertEqual(27, user.get('age'))

    def test_apply_none_data(self):
        # given:
        string_converter = StringConverter()
        cell_conversion = DirectCellConversion('product.id', string_converter)

        # when:
        data_node = DataNode(defaults={
            conversion_strategy.CONTENT_FIELD: {
                'product': {
                    'name': 'product name'
                }
            }
        })
        cell_conversion.apply(data_node, None)

        # then:
        content = data_node.as_dict().get(conversion_strategy.CONTENT_FIELD)
        self.assertIsNotNone(content)

        # and:
        product = content.get('product')
        self.assertIsNotNone(product)
        self.assertTrue('id' not in product, '[id] not expected to be in product field')


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
        content = data_node.as_dict().get(conversion_strategy.CONTENT_FIELD)
        self.assertIsNotNone(content)

        # and:
        list_of_things = content.get('list_of_things')
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
        data_node[f'{conversion_strategy.CONTENT_FIELD}.user.basket'] = [{'quantity': 3}]

        # when:
        cell_conversion.apply(data_node, 'apple')

        # then:
        content = data_node.as_dict().get(conversion_strategy.CONTENT_FIELD)
        self.assertIsNotNone(content)

        # and:
        basket = content.get('user').get('basket')
        self.assertEqual(1, len(basket))

        # and:
        current_element = basket[0]
        self.assertEqual(3, current_element.get('quantity'))
        self.assertEqual('apple - converted', current_element.get('product_name'))

    def test_apply_none_data(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = ListElementCellConversion('user.name', converter)

        # and:
        data_node = DataNode(defaults={
            conversion_strategy.CONTENT_FIELD: {
                'user': [{'id': '65fd8'}]
            }
        })

        # when:
        cell_conversion.apply(data_node, None)

        # then:
        list_element = data_node[f'{conversion_strategy.CONTENT_FIELD}.user'][0]
        self.assertTrue('name' not in list_element.keys(), '[name] should not be added to element.')


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
        expected_id = 'product_no_144 - converted'
        self.assertEqual(data_node[conversion_strategy.OBJECT_ID_FIELD], expected_id)


class LinkedIdentityCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = LinkedIdentityCellConversion('item.item_id', converter)

        # and:
        data_node = DataNode()

        # when:
        cell_conversion.apply(data_node, 'item_no_29')

        # then:
        links = data_node[conversion_strategy.LINKS_FIELD]
        self.assertIsNotNone(links)

        # and:
        items = links.get('item')
        self.assertEqual(1, len(items))
        self.assertTrue('item_no_29 - converted' in items)

    def test_apply_with_previous_entry(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = LinkedIdentityCellConversion('item.item_number', converter)

        # and:
        data_node = DataNode()
        items = ['item_no_56', 'item_no_199']
        data_node[conversion_strategy.LINKS_FIELD] = {'item': items}

        # when:
        cell_conversion.apply(data_node, 'item_no_721')

        # then:
        actual_items = data_node[conversion_strategy.LINKS_FIELD]['item']
        self.assertEqual(3, len(actual_items))

        # and:
        expected_ids = [id for id in items]
        expected_ids.append('item_no_721 - converted')
        for expected_id in expected_ids:
            self.assertTrue(expected_id in actual_items, f'[{expected_id}] not in list.')

