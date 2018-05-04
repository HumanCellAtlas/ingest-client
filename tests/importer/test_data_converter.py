from unittest import TestCase

from ingest.importer.data_converter import BooleanConverter


class DataConverterTest(TestCase):

    def test_convert(self):
        converter = BooleanConverter()

        self.assertTrue(converter.convert('true'))
        self.assertTrue(converter.convert('True'))
        self.assertFalse(converter.convert('false'))
        self.assertFalse(converter.convert('False'))

