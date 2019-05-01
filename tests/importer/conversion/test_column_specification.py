from unittest import TestCase

import copy

from mock import MagicMock

from ingest.importer.conversion import column_specification
from ingest.importer.conversion.column_specification import ColumnSpecification, ConversionType
from ingest.importer.conversion.data_converter import DataType, IntegerConverter, \
    BooleanConverter, ListConverter, StringConverter, DefaultConverter
from ingest.template.schema_template import UnknownKeyException


class ColumnSpecificationTest(TestCase):

    def test_look_up_id_field(self):
        # given:
        schema_spec = {
            'product.id': {'value_type': 'integer', 'multivalue': False, 'identifiable': True}
        }
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        id_spec = column_specification.look_up(schema_template, 'product.id', 'product')

        # then:
        self.assertTrue(id_spec.is_identity())
        self.assertEqual('product', id_spec.context_concrete_type)
        self.assertEqual('merchandise', id_spec.domain_type)
        self.assertEqual('product.id', id_spec.field_name)
        self.assertEqual(DataType.INTEGER, id_spec.data_type)

    def test_look_up_object_field(self):
        # given:
        schema_spec = {'product.name': {'value_type': 'string', 'multivalue': False}}
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        name_spec = column_specification.look_up(schema_template, 'product.name', 'product',
                                                 order_of_occurrence=7)

        # then:
        self.assertFalse(name_spec.multivalue)
        self.assertEqual('product.name', name_spec.field_name)
        self.assertEqual('product', name_spec.context_concrete_type)
        self.assertEqual('merchandise', name_spec.domain_type)
        self.assertEqual(DataType.STRING, name_spec.data_type)
        self.assertEqual(7, name_spec.order_of_occurrence)

    def test_look_up_linked_object_field(self):
        # given:
        schema_spec = {'manufacturer.organisation': {'value_type': 'string', 'multivalue': False}}
        schema_template = self._prepare_mock_schema_template('manufacturer',
                                                             'manufacturer/manufacturer',
                                                             schema_spec)

        # when:
        organisation_spec = column_specification.look_up(schema_template,
                                                         'manufacturer.organisation', 'product')

        # then:
        self.assertEqual('manufacturer', organisation_spec.domain_type)
        self.assertEqual('product', organisation_spec.context_concrete_type)

    def test_look_up_multivalue_object_field(self):
        # given:
        schema_spec = {'product.remarks': {'value_type': 'string', 'multivalue': True}}
        schema_template = self._prepare_mock_schema_template('product', 'merchandise/product',
                                                             schema_spec)

        # when:
        remarks_spec = column_specification.look_up(schema_template, 'product.remarks', 'product')

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
        review_rating_spec = column_specification.look_up(schema_template, 'product.reviews.rating',
                                                          'product')

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
                                                            'product.manufacturer_id', 'product')

        # then:
        self.assertTrue(manufacturer_id_spec.identity)
        self.assertTrue(manufacturer_id_spec.external_reference)
        self.assertEqual('product.manufacturer_id', manufacturer_id_spec.field_name)
        self.assertEqual(DataType.INTEGER, manufacturer_id_spec.data_type)

    def test_look_up_with_field_anchor(self):
        # given:
        schema_spec = {
            'user.sn_profiles.account_id': {'value_type': 'string', 'multivalue': False},
            'user.sn_profiles': {
                'value_type': 'object',
                'multivalue': True
            }
        }
        schema_template = self._prepare_mock_schema_template('user', 'user/user', schema_spec)

        # when:
        account_id_spec = column_specification.look_up(schema_template,
                                                       'user.sn_profiles.account_id', 'user',
                                                       context='user.sn_profiles')
        account_id_user_spec = column_specification.look_up(schema_template,
                                                            'user.sn_profiles.account_id', 'user',
                                                            context='user')

        # then:
        self.assertFalse(account_id_spec.is_field_of_list_element())
        self.assertEqual(DataType.STRING, account_id_spec.data_type)
        self.assertFalse(account_id_spec.multivalue)

        # and:
        self.assertTrue(account_id_user_spec.is_field_of_list_element())

    def test_look_up_unknown_header(self):
        # given:
        schema_template = MagicMock(name='schema_template')
        schema_template.lookup = MagicMock(side_effect=UnknownKeyException())

        # when:
        spec = column_specification.look_up(schema_template, 'product.info.unknown', 'product')

        # then:
        self.assertIsNotNone(spec)
        self.assertEqual(column_specification.UNKNOWN_DOMAIN_TYPE, spec.domain_type)

    @staticmethod
    def _prepare_mock_schema_template(domain_type, domain_entity=None, schema_spec_map=None):
        value_map = copy.deepcopy(schema_spec_map)
        if domain_entity:
            type_spec = {'schema': {'domain_entity': domain_entity}}
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
                                          DataType.STRING, identity=True, order_of_occurrence=2)

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
