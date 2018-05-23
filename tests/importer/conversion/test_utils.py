from unittest import TestCase

from ingest.importer.conversion import utils


class ModuleTest(TestCase):

    def test_split_field_chain(self):
        # given:
        single = 'field'
        double = 'user.name'
        triple = 'user.address.city'

        # when:
        single_parent, single_target = utils.split_field_chain(single)
        double_parent, double_target = utils.split_field_chain(double)
        triple_parent, triple_target = utils.split_field_chain(triple)

        # then:
        self.assertEqual('', single_parent)
        self.assertEqual('field', single_target)

        # and:
        self.assertEqual('user', double_parent)
        self.assertEqual('name', double_target)

        # and:
        self.assertEqual('user.address', triple_parent)
        self.assertEqual('city', triple_target)

    def test_extract_root_field(self):
        # given:
        single = 'user'
        double = 'account.number'
        triple = 'product.item.id'

        # expect:
        self.assertEqual('user', utils.extract_root_field(single))
        self.assertEqual('account', utils.extract_root_field(double))
        self.assertEqual('product', utils.extract_root_field(triple))
        self.assertEqual('', utils.extract_root_field(''))
        self.assertIsNone(utils.extract_root_field(None))
