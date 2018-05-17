from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion.conversion_strategy import DirectCellConversion, \
    ListElementCellConversion
from ingest.importer.data_node import DataNode


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


class ListElementCellConversionTest(TestCase):

    def test_apply(self):
        # given:
        converter = MagicMock('converter')
        converter.convert = lambda data: f'{data} - converted'

        # and:
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
