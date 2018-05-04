from unittest import TestCase

from ingest.importer.data_converter import BooleanConverter, InvalidBooleanValue


class DataConverterTest(TestCase):

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
