from unittest import TestCase

from ingest.importer.conversion.metadata_entity import MetadataEntity


class MetadataEntityTest(TestCase):

    def test_define_content(self):
        # given:
        metadata = MetadataEntity()

        # when:
        metadata.define_content('user.name', 'Juan dela Cruz')
        metadata.define_content('user.age', 47)

        # then:
        user = metadata.get_content('user')
        self.assertIsNotNone(user)
        self.assertEqual('Juan dela Cruz', user.get('name'))
        self.assertEqual(47, user.get('age'))

    def test_add_links(self):
        # given:
        metadata = MetadataEntity()

        # when:
        new_links = ['73f909', '83fddf1', '9004811']
        metadata.add_links('profile', new_links)

        # then:
        profile_links = metadata.get_links('profile')
        self.assertIsNotNone(profile_links)
        self.assertCountEqual(new_links, profile_links)

    def test_add_links_multiple_times(self):
        # given:
        metadata = MetadataEntity()

        # and:
        batch_1 = ['78de112', '963fefed']
        batch_2 = ['2daab01']

        # when:
        metadata.add_links('item', batch_1)
        metadata.add_links('item', batch_2)

        # then:
        item_links = metadata.get_links('item')
        self.assertIsNotNone(item_links)
        self.assertCountEqual(batch_1 + batch_2, item_links)

    def test_add_external_links(self):
        # given:
        metadata = MetadataEntity()

        # when:
        new_links = ['77701ee', '254aefb']
        metadata.add_external_links('file', new_links)

        # then:
        file_links = metadata.get_external_links('file')
        self.assertIsNotNone(file_links)
        self.assertCountEqual(new_links, file_links)

    def test_add_external_links_multiple_times(self):
        # given:
        metadata = MetadataEntity()

        # and:
        batch_1 = ['f8ede49']
        batch_2 = ['3dbb8b8']
        batch_3 = ['c23c45c']

        # when:
        metadata.add_external_links('process', batch_1)
        metadata.add_external_links('process', batch_2)
        metadata.add_external_links('process', batch_3)

        # then:
        process_links = metadata.get_external_links('process')
        self.assertIsNotNone(process_links)
        self.assertCountEqual(batch_2 + batch_1 + batch_3, process_links)

    def test_define_linking_detail(self):
        # given:
        metadata = MetadataEntity()

        # when:
        metadata.define_linking_detail('product_core.name', 'Apple Juice')
        metadata.define_linking_detail('product_core.description', 'pasteurised fruit juice')

        # then:
        product_core = metadata.get_linking_detail('product_core')
        self.assertIsNotNone(product_core)
        self.assertEqual('Apple Juice', product_core.get('name'))
        self.assertEqual('pasteurised fruit juice', product_core.get('description'))

    def test_retain_content_fields(self):
        # given:
        content = {'user_name': 'jdelacruz', 'password': 'dontrevealthis', 'description': 'temp'}
        metadata_entity = MetadataEntity(domain_type='user', concrete_type='user', object_id=1,
                                         content=content)
        # when:
        metadata_entity.retain_fields('user_name')

        # then:
        self.assertEqual(['user_name'], list(metadata_entity.content.as_dict().keys()))

    def test_add_module_entity(self):
        # given:
        product = MetadataEntity(domain_type='product', concrete_type='product', object_id=12,
                                 content={'name': 'test product'})
        john_review = MetadataEntity(domain_type='product', concrete_type='product', object_id=12,
                                     content={'reviews': {'user': 'john', 'rating': 5}})
        mary_review = MetadataEntity(domain_type='product', concrete_type='product', object_id=12,
                                     content={'reviews': {'user': 'mary', 'rating': 3}})

        # when:
        product.add_module_entity(john_review)
        product.add_module_entity(mary_review)

        # then:
        content_reviews = product.content['reviews']
        self.assertIsNotNone(content_reviews)
        self.assertEqual(2, len(content_reviews))

        # and:
        self.assertEqual({'user': 'john', 'rating': 5}, content_reviews[0])
        self.assertEqual({'user': 'mary', 'rating': 3}, content_reviews[1])

    def test_map_for_submission(self):
        # given:
        test_content = {'description': 'test'}
        test_links = {'items': ['123', '456']}
        test_external_links = {'producer': ['abc', 'def', 'ghi']}
        test_linking_details = {'link': 'details', 'test': '123'}
        metadata_entity = MetadataEntity(concrete_type='warehouse', content=test_content,
                                         links=test_links,
                                         external_links=test_external_links,
                                         linking_details=test_linking_details)

        # when:
        submission_dict = metadata_entity.map_for_submission()

        # then:
        self.assertEqual('warehouse', submission_dict.get('concrete_type'))
        self.assertEqual(test_content, submission_dict.get('content'))
        self.assertEqual(test_links, submission_dict.get('links_by_entity'))
        self.assertEqual(test_external_links, submission_dict.get('external_links_by_entity'))
        self.assertEqual(test_linking_details, submission_dict.get('linking_details'))
