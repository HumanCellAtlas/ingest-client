from unittest import TestCase

from ingest.importer.data_node import DataNode


class DataNodeTest(TestCase):

    def test___setitem__(self):
        # given:
        node = DataNode()

        # when:
        node['path.to.node'] = 'value'

        # then:
        self.assertEqual('value', node.as_dict()['path']['to']['node'])
