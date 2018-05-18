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
        data_node = DataNode(defaults=defaults)

        # expect:
        self.assertEqual('Juan', data_node['first_name'])
        self.assertEqual(39, data_node['age'])

        # and:
        self.assertEqual('Manila', data_node['address.city'])
        self.assertEqual('Philippines', data_node['address.country'])

    def test___getitem___non_existent_path(self):
        # given:
        defaults = {'product': {'name': 'biscuit', 'id': '123'}}
        data_node = DataNode(defaults=defaults)

        # expect:
        # TODO this should probably throw exception instead, indicating path does not exist
        self.assertIsNone(data_node['product.path.does.not.exist'])
        self.assertIsNone(data_node['simply.does.not.exist'])
