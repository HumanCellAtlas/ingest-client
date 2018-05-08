from unittest import TestCase

from mock import MagicMock

from ingest.importer.conversion.template_manager import TemplateManager
from ingest.importer.schematemplate import SchemaTemplate


class TemplateManagerTest(TestCase):

    def test_get_converter(self):
        # given:
        schema_template = SchemaTemplate()
        short_name_spec = {
            'project_short_name': {
                'display_name': 'Short name',
                'description': 'A unique label for the project.',
                'value_type': 'string',
                'multivalue': False
            }
        }
        schema_template.lookup = MagicMock(name='lookup', return_value=short_name_spec)

        # and:
        template_manager = TemplateManager(schema_template)

        # when:
        converter = template_manager.get_converter('path.to.field.project_shortname')

        # then:
        self.assertIsNotNone(converter)
