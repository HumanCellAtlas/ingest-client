from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion import conversion_strategy
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.conversion_strategy import DirectCellConversion, \
    ListElementCellConversion, CellConversion, IdentityCellConversion, LinkedIdentityCellConversion, \
    DoNothing, ExternalReferenceCellConversion, LinkingDetailCellConversion, \
    FieldOfSingleElementListCellConversion
from ingest.importer.conversion.data_converter import StringConverter, ListConverter
from ingest.importer.conversion.exceptions import UnknownMainCategory
from ingest.importer.conversion.metadata_entity import MetadataEntity


def _mock_column_spec(field_name='field_name', main_category=None, converter=None,
                      conversion_type=ConversionType.UNDEFINED):
    column_spec: ColumnSpecification = MagicMock('column_spec')
    column_spec.field_name = field_name
    column_spec.main_category = main_category
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
                                      ListElementCellConversion,
                                      expected_converter_type=ListConverter)

    def test_determine_strategy_for_identity_field(self):
        # expect:
        self._assert_correct_strategy(ConversionType.IDENTITY, IdentityCellConversion)

    def test_determine_strategy_for_linking_detail_field(self):
        # expect
        self._assert_correct_strategy(ConversionType.LINKING_DETAIL, LinkingDetailCellConversion)

    def test_determine_strategy_for_linked_identity_field(self):
        # expect:
        self._assert_correct_strategy(ConversionType.LINKED_IDENTITY, LinkedIdentityCellConversion,
                                      expected_converter_type=ListConverter,
                                      and_also=self._assert_correct_main_category)

    def test_determine_strategy_for_external_reference_field(self):
        self._assert_correct_strategy(ConversionType.EXTERNAL_REFERENCE,
                                      ExternalReferenceCellConversion,
                                      expected_converter_type=ListConverter,
                                      and_also=self._assert_correct_main_category)

    def _assert_correct_main_category(self, strategy: CellConversion):
        self.assertEqual('product_type', strategy.main_category)

    def _assert_correct_strategy(self, conversion_type, strategy_class,
                                 expected_converter_type=None, and_also=None):
        # given:
        converter = MagicMock('converter')
        column_spec = _mock_column_spec(field_name='product.product_id',
                                        main_category='product_type', converter=converter,
                                        conversion_type=conversion_type)

        # when:
        strategy: CellConversion = conversion_strategy.determine_strategy(column_spec)

        # then:
        self.assertIsInstance(strategy, strategy_class)
        self.assertEqual('product.product_id', strategy.field)

        # and:
        if expected_converter_type is None:
            self.assertEqual(converter, strategy.converter)
        else:
            self.assertIsInstance(strategy.converter, expected_converter_type)

        # and:
        if and_also is not None:
            and_also(strategy)

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
        cell_conversion = DirectCellConversion('profile.user.age', int_converter)

        # when:
        metadata = MetadataEntity()
        cell_conversion.apply(metadata, '27')

        # then:
        self.assertEqual(27, metadata.get_content('user.age'))

    def test_apply_skips_none_data(self):
        # given:
        string_converter = StringConverter()
        cell_conversion = DirectCellConversion('product.id', string_converter)

        # when:
        metadata = MetadataEntity(content={
            'product': {
                'name': 'product name'
            }
        })
        cell_conversion.apply(metadata, None)

        # then:
        product = metadata.get_content('product')
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
        cell_conversion = ListElementCellConversion('stuff.list_of_things.name', converter)

        # when:
        metadata = MetadataEntity()
        cell_conversion.apply(metadata, 'sample')

        # then:
        list_of_things = metadata.get_content('list_of_things')
        self.assertIsNotNone(list_of_things)
        self.assertEqual(1, len(list_of_things))

        # and:
        thing = list_of_things[0]
        self.assertEqual('sample - converted', thing.get('name'))

    def test_apply_previously_processed_field(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = ListElementCellConversion('shop.user.basket.product_name', converter)

        # and:
        metadata = MetadataEntity()
        metadata.define_content('user.basket', [{'quantity': 3}])

        # when:
        cell_conversion.apply(metadata, 'apple')

        # then:
        basket = metadata.get_content('user.basket')
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
        metadata = MetadataEntity(content={
            'user': [{'id': '65fd8'}]
        })

        # when:
        cell_conversion.apply(metadata, None)

        # then:
        list_element = metadata.get_content('user')[0]
        self.assertTrue('name' not in list_element.keys(), '[name] should not be added to element.')

    def test_apply_multiple(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = ListElementCellConversion('group.members.name', converter)

        # when:
        metadata = MetadataEntity()
        cell_conversion.apply(metadata, 'Juan||Pedro||Jane')

        # then:
        members = metadata.get_content('members')
        self.assertEqual(3, len(members))

        # and:
        expected_names = ['Juan - converted', 'Pedro - converted', 'Jane - converted']
        for member in members:
            member_name = member.get('name')
            self.assertIn(member_name, expected_names)

    def test_apply_multiple_with_previous_values(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = ListElementCellConversion('staff.developers.name', converter)

        # and:
        metadata = MetadataEntity()
        metadata.define_content('developers', [{'age': 32}, {'age': 23}])

        # when:
        cell_conversion.apply(metadata, 'John||Paul||George||Ringo')

        # then:
        developers = metadata.get_content('developers')
        self.assertEqual(4, len(developers))

        # and:
        expected_names = ['John - converted', 'Paul - converted', 'George - converted',
                          'Ringo - converted']
        expected_ages = [32, 23, None, None]
        for index, developer in enumerate(developers):
            self.assertEqual(expected_names[index], developer.get('name'))
            self.assertEqual(expected_ages[index], developer.get('age'))


class FieldOfSingleElementListCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = FieldOfSingleElementListCellConversion('library.books.title', converter)

        # when:
        metadata = MetadataEntity()
        cell_conversion.apply(metadata, 'Harry Potter')

        # then:
        books = metadata.get_content('books')
        self.assertIsNotNone(books)
        self.assertEqual(1, len(books))

        # and:
        thing = books[0]
        self.assertEqual('Harry Potter - converted', thing.get('title'))

    def test_apply_multiple_value_field(self):
        # given:
        converter = ListConverter(base_converter=_create_mock_string_converter())
        cell_conversion = FieldOfSingleElementListCellConversion('library.books.authors', converter)

        # and:
        metadata = MetadataEntity(content={
            'books': [
                {'title': 'Once Upon a Time'}
            ]
        })

        # when:
        cell_conversion.apply(metadata, 'J dela Cruz||P Smith')

        # then:
        books = metadata.get_content('books')
        self.assertEqual(1, len(books))

        # and:
        book = books[0]
        self.assertEqual(['J dela Cruz - converted', 'P Smith - converted'], book.get('authors'))


class IdentityCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = IdentityCellConversion('product.product_id', converter)

        # and:
        metadata = MetadataEntity()

        # when:
        cell_conversion.apply(metadata, 'product_no_144')

        # then:
        expected_id = 'product_no_144 - converted'
        self.assertEqual(expected_id, metadata.object_id)

        # and: identity value should be in content
        self.assertEqual(expected_id, metadata.get_content('product_id'))


class LinkedIdentityCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        cell_conversion = LinkedIdentityCellConversion('item.item_id', 'item_type')

        # and:
        metadata = MetadataEntity()

        # when:
        cell_conversion.apply(metadata, 'item_no_29')
        cell_conversion.apply(metadata, 'item_no_31||item_no_50')

        # then:
        item_types = metadata.get_links('item_type')
        self.assertEqual(3, len(item_types))

        # and:
        expected_items = [f'item_no_{number}' for number in [29, 31, 50]]
        for expected_item in expected_items:
            self.assertTrue(expected_item in item_types, f'[{expected_item}] not in list.')

    def test_apply_with_previous_entry(self):
        # given:
        cell_conversion = LinkedIdentityCellConversion('item.item_number', 'line_order')

        # and:
        metadata = MetadataEntity()
        items = ['item_no_56', 'item_no_199']
        metadata.add_links('line_order', items)

        # when:
        cell_conversion.apply(metadata, 'item_no_721')

        # then:
        actual_items = metadata.get_links('line_order')
        self.assertEqual(3, len(actual_items))

        # and:
        expected_ids = [id for id in items]
        expected_ids.append('item_no_721')
        for expected_id in expected_ids:
            self.assertTrue(expected_id in actual_items, f'[{expected_id}] not in list.')

    def test_apply_no_main_category(self):
        # given:
        cell_conversion = LinkedIdentityCellConversion('product.name', None)

        # when:
        exception_thrown = False
        try:
            cell_conversion.apply(MetadataEntity(), 'sample')
        except UnknownMainCategory:
            exception_thrown = True

        # then:
        self.assertTrue(exception_thrown, f'[{UnknownMainCategory.__name__}] not raised.')


class ExternalReferenceCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        cell_conversion = ExternalReferenceCellConversion('user.uuid', 'account')

        # when:
        metadata = MetadataEntity()
        cell_conversion.apply(metadata, '621bfa0')

        # then:
        account_list = metadata.get_external_links('account')
        self.assertIsNotNone(account_list, '[account] list in external links expected.')
        self.assertEqual(1, len(account_list))
        self.assertTrue('621bfa0' in account_list, 'Expected content not in list.')

    def test_apply_multiple_values(self):
        # given:
        cell_conversion = ExternalReferenceCellConversion('company.uuid', 'organisation')

        # when:
        metadata = MetadataEntity()
        cell_conversion.apply(metadata, '7e56de9||2fe9eb0')

        # then:
        expected_ids = ['7e56de9', '2fe9eb0']
        self.assertCountEqual(expected_ids, metadata.get_external_links('organisation'))

    def test_apply_with_previous_entries(self):
        # given:
        metadata = MetadataEntity()
        metadata.add_external_links('store_item', ['109bdd9', 'c3c35e6'])

        # and:
        cell_conversion = ExternalReferenceCellConversion('product.uuid', 'store_item')

        # when:
        cell_conversion.apply(metadata, '73de901')

        # then:
        store_item_list = metadata.get_external_links('store_item')
        self.assertIsNotNone(store_item_list, '[store_item] list in external links expected.')

        # and:
        expected_ids = ['109bdd9', '73de901', 'c3c35e6']
        self.assertCountEqual(expected_ids, store_item_list)


class LinkingDetailCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = LinkingDetailCellConversion('profile.user.name', converter)
        metadata = MetadataEntity()

        # when:
        cell_conversion.apply(metadata, 'John Doe')

        # then:
        self.assertEqual('John Doe - converted', metadata.get_linking_detail('user.name'))

    def test_apply_with_previous_entries(self):
        # given:
        converter = _create_mock_string_converter()
        cell_conversion = LinkingDetailCellConversion('product.item.name', converter)

        # and:
        metadata = MetadataEntity(linking_details={
            'item': {'description': 'a jar of milk'}
        })

        # when:
        cell_conversion.apply(metadata, 'milk')

        # then:
        self.assertEqual('milk - converted', metadata.get_linking_detail('item.name'))
        self.assertEqual('a jar of milk', metadata.get_linking_detail('item.description'))
