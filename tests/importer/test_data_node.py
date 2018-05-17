from unittest import TestCase

from ingest.importer.data_node import DataNode


class DataNodeTest(TestCase):

    def test___setitem__(self):
        # given:
        node = DataNode()

        # when:
        node['path.to.node'] = 'value'
        node['path.to.nested.field'] = 347

        # then:
        dict = node.as_dict()
        self.assertEqual('value', dict['path']['to']['node'])
        self.assertEqual(347, dict['path']['to']['nested']['field'])

    def test___getitem__(self):
        # given:
        defaults = {
            'first_name': 'Juan',
            'last_name': 'dela Cruz',
            'age': 39,
            'address': {
                'city': 'Manila',
                'country': 'Philippines'
            }
        }

        # and:
        node = DataNode(defaults=defaults)

        # expect:
        self.assertEqual('Juan', node['first_name'])
