from unittest import TestCase

from ingest.importer.conversion.data_converter import (
    BooleanConverter, ListConverter, DataType, IntegerConverter,
    StringConverter)
from ingest.importer.conversion.exceptions import InvalidBooleanValue


class DataTypeTest(TestCase):

    def test_find_case_insensitive(self):
        # expect:
        self.assertEqual(DataType.STRING, DataType.find('string'))
        self.assertEqual(DataType.STRING, DataType.find('String'))
        self.assertEqual(DataType.STRING, DataType.find('STRING'))

    def test_find_not_found(self):
        # expect:
        self.assertEqual(DataType.UNDEFINED, DataType.find('does_not_exist'))
        self.assertEqual(DataType.UNDEFINED, DataType.find(None))


class StringConverterTest(TestCase):

    def test_convert(self):
        # given:
        converter = StringConverter()

        # expect:
        self.assertEqual('data', converter.convert('data'))
        self.assertEqual('278', converter.convert(278))
        self.assertEqual('True', converter.convert(True))
        self.assertEqual('3.1416', converter.convert(3.1416))


class IntegerConverterTest(TestCase):

    def test_convert_from_string(self):
        # given:
        converter = IntegerConverter()

        # expect:
        self.assertEqual(37, converter.convert('37'))
        self.assertEqual(190, converter.convert('190'))

    def test_convert_from_float(self):
        # given:
        converter = IntegerConverter()

        # expect:
        self.assertEqual(899, converter.convert(899.003))
        self.assertEqual(200934, converter.convert(200934.118))

    def test_convert_from_integer(self):
        # given:
        converter = IntegerConverter()

        # expect:
        self.assertEqual(755, converter.convert(755))
        self.assertEqual(89221, converter.convert(89221))


class BooleanConverterTest(TestCase):

    def test_convert_to_false(self):
        converter = BooleanConverter()
        self.assertFalse(converter.convert('false'))
        self.assertFalse(converter.convert('False'))
        self.assertFalse(converter.convert('no'))
        self.assertFalse(converter.convert('No'))

    def test_convert_to_true(self):
        converter = BooleanConverter()
        self.assertTrue(converter.convert('true'))
        self.assertTrue(converter.convert('True'))
        self.assertTrue(converter.convert('yes'))
        self.assertTrue(converter.convert('Yes'))

    def test_convert_invalid_value(self):
        converter = BooleanConverter()

        with self.assertRaises(InvalidBooleanValue) as context:
            converter.convert('yup')

        self.assertEqual('yup', context.exception.get_value())


class ListConverterTest(TestCase):

    def test_convert_to_string_list(self):
        converter = ListConverter()
        self.assertEqual(['apple', 'banana', 'carrot'], converter.convert('apple||banana||carrot'))
        self.assertEqual(['durian', 'eggfruit', 'fig'], converter.convert('durian||eggfruit||fig'))

    def test_convert_to_int_list(self):
        converter = ListConverter(data_type=DataType.INTEGER)
        self.assertEqual([1, 2, 3], converter.convert('1||2||3'))

    def test_convert_to_int_list_multiple(self):
        converter = ListConverter(data_type=DataType.INTEGER)
        self.assertEqual([1, 2, 3], converter.convert('1||2||3'))

    def test_convert_to_int_list_single(self):
        converter = ListConverter(data_type=DataType.INTEGER)
        self.assertEqual([9606], converter.convert(9606))
