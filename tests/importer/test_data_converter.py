from unittest import TestCase

from ingest.importer.data_converter import BooleanConverter


class DataConverterTest(TestCase):

    def test_convert(self):
        converter = BooleanConverter()

        value = converter.convert('true')

        self.assertTrue(value)

        value = converter.convert('false')

        self.assertFalse(value)




