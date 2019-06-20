import copy
import logging

from openpyxl.worksheet.worksheet import Worksheet

import ingest.template.schema_template as schema_template
from ingest.api.ingestapi import IngestApi
from ingest.importer.conversion import conversion_strategy, column_specification
from ingest.importer.conversion.conversion_strategy import CellConversion
from ingest.importer.conversion.metadata_entity import MetadataEntity
from ingest.importer.data_node import DataNode
from ingest.importer.spreadsheet.ingest_worksheet import IngestWorksheet, \
    MODULE_TITLE_PATTERN, IngestRow
from ingest.template.schema_template import SchemaTemplate


class TemplateManager:

    def __init__(self, template: SchemaTemplate, ingest_api: IngestApi):
        self.template = template
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)

    def create_template_node(self, worksheet: Worksheet):
        concrete_entity = self.get_concrete_type(worksheet.title)
        schema = self._get_schema(concrete_entity)
        data_node = DataNode()
        data_node['describedBy'] = schema['url']
        data_node['schema_type'] = schema['domain_entity']
        return data_node

    def create_row_template(self, ingest_worksheet: IngestWorksheet):
        concrete_type = self.get_concrete_type(ingest_worksheet.title)
        domain_type = self.get_domain_type(concrete_type)
        column_headers = ingest_worksheet.get_column_headers()
        cell_conversions = []

        context = self._determine_context(concrete_type, ingest_worksheet)
        header_counter = {}
        for header in column_headers:
            if not header_counter.get(header):
                header_counter[header] = 0
            header_counter[header] = header_counter[header] + 1

            column_spec = column_specification.look_up(self.template, header, concrete_type,
                                                       context=context,
                                                       order_of_occurrence=header_counter[header])
            strategy = conversion_strategy.determine_strategy(column_spec)
            cell_conversions.append(strategy)

        default_values = self._define_default_values(concrete_type)
        return RowTemplate(domain_type, concrete_type, cell_conversions,
                           default_values=default_values)

    @staticmethod
    def _determine_context(concrete_type, ingest_worksheet):
        context_components = [concrete_type]
        module_field_name = ingest_worksheet.get_module_field_name()
        if module_field_name:
            context_components.append(module_field_name)
        context = '.'.join(context_components)
        return context

    def _define_default_values(self, object_type):
        default_values = {
            'describedBy': self.get_schema_url(object_type),
            'schema_type': self.get_domain_type(object_type)
        }
        return default_values

    def get_latest_schema_url(self, high_level_entity, domain_entity, concrete_entity):
        latest_schema = self.ingest_api.get_schemas(
            latest_only=True,
            high_level_entity=high_level_entity,
            domain_entity=domain_entity.split('/')[0],
            concrete_entity=concrete_entity
        )

        return latest_schema[0]['_links']['json-schema']['href'] if latest_schema else None

    def get_schema_url(self, concrete_entity):
        schema = self._get_schema(concrete_entity)
        # TODO use schema version that is specified in spreadsheet for now
        return schema.get('url') if schema else None

    # TODO this just 2 lines. Perhaps we can just inline this to client code?
    def _get_schema(self, concrete_entity):
        spec = self.lookup(concrete_entity)
        return spec.get('schema') if spec else None

    def get_concrete_type(self, title):
        """
        Concrete Type refers to the specific type of an object based on a given schema.
        This method determines the concrete type given the worksheet title.

        :param title: the title of the worksheet.
        :return: the Concrete Type of a given worksheet title
        """
        result = MODULE_TITLE_PATTERN.search(title)
        if not result:
            raise InvalidTabName(title)
        main_label = result.group('main_label')
        return self.template.get_tab_key(main_label)

    def get_domain_type(self, concrete_type):
        """
        Domain Entity Type is the high level classification of Concrete Entities. For example,
        Donor Organism belongs to the Biomaterial domain; all Donor Organisms are considered
        Biomaterials.

        :param concrete_type: the actual metadata entity type
        :return: the Domain Entity Type for the given concrete_entity
        """
        domain_type = None
        spec = self.lookup(concrete_type)
        schema = spec.get('schema') if spec else None
        if schema:
            domain_type = schema.get('domain_entity', '')
            type_components = domain_type.split('/')
            if type_components:
                domain_type = type_components[0]
        return domain_type

    # Created this convenience method so that clients don't have to call template manager twice.
    # Unlike _get_schema, this will be used outside the context of this class.
    def get_worksheet_domain_type(self, title):
        concrete_type = self.get_concrete_type(title)
        return self.get_domain_type(concrete_type)

    def get_key_for_label(self, header_name, tab_name):
        try:
            key = self.template.get_key_for_label(header_name, tab_name)
        except Exception:
            self.logger.warning(f'{header_name} in "{tab_name}" tab is not found in schema template')
        return key

    def lookup(self, header_name):
        try:
            spec = self.template.lookup(header_name)
        except schema_template.UnknownKeyException:
            self.logger.warning(f'schema_template.UnknownKeyException: Could not lookup {header_name} in template.')
            return {}

        return spec


def build(schemas, ingest_api) -> TemplateManager:
    template = None

    if not schemas:
        template = SchemaTemplate(ingest_api_url=ingest_api.url)
    else:
        template = SchemaTemplate(ingest_api_url=ingest_api.url, list_of_schema_urls=schemas)

    template_mgr = TemplateManager(template, ingest_api)
    return template_mgr


class RowTemplate:

    def __init__(self, domain_type, object_type, cell_conversions, default_values={}):
        self.domain_type = domain_type
        self.concrete_type = object_type
        self.cell_conversions = cell_conversions
        self.default_values = copy.deepcopy(default_values)

    def do_import(self, row: IngestRow):
        metadata = MetadataEntity(domain_type=self.domain_type, concrete_type=self.concrete_type,
                                  content=self.default_values, row=row)
        for index, cell in enumerate(row.values):
            if cell.value is None:
                continue
            conversion: CellConversion = self.cell_conversions[index]
            conversion.apply(metadata, cell.value)
        return metadata


class ParentFieldNotFound(Exception):
    def __init__(self, header_name):
        message = f'{header_name} has no parent field'
        super(ParentFieldNotFound, self).__init__(message)

        self.header_name = header_name


class InvalidTabName(Exception):

    def __init__(self, tab_name):
        super(InvalidTabName, self).__init__(f'Invalid tab name [{tab_name}]')
        self.tab_name = tab_name
