from unittest import TestCase

from ingest.importer.conversion.data_converter import BooleanConverter, InvalidBooleanValue, ListConverter


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
        self.assertEqual(['durian', 'elderberry', 'fig'], converter.convert('durian||elderberry||fig'))

    def test_convert_to_int_list(self):
        converter = ListConverter(data_type='integer')
        self.assertEqual([1, 2, 3], converter.convert('1||2||3'))
