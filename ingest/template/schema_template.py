from datetime import datetime

import requests
import yaml

from ingest.api.ingestapi import IngestApi
from ingest.template.descriptor import SimplePropertyDescriptor
from .exceptions import RootSchemaException, UnknownKeySchemaException
from .migration_parser import MigrationParser
from .schema_parser import SchemaParser
from .tab_config import TabConfig

EXTERNAL_REFERENCE_FIELD = 'uuid'


class SchemaTemplate():
    """ A SchemaTemplate is used to encapsulate information about all the metadata schema files and
    property migration files that are directly passed in in order to generate a spreadsheet. """

    def __init__(self, ingest_api_url="http://api.ingest.dev.data.humancellatlas.org",
                 migrations_url="https://schema.dev.data.humancellatlas.org/property_migrations",
                 metadata_schema_urls=None, json_schema_docs=None, tab_config=None, property_migrations=None,
                 custom_properties=None):
        """ Creates and empty/default dictionary containing the following information:
        1) template_version:  A string keeping track of the version of the TopLevelSchemaDescriptor that is being used
        in case the components of this dictionary changes over time.

        2) created_date: A string representing the date and time at which point this internal representation of the
        metadata schemas and property migration file was created.

        3) meta_data_properties: A dictionary structure where each key represents a metadata schema that was directly
        passed in to parse and where each value is a dictionary representing any properties that are associated with
        the given schema. The dictionary itself may be recursive and contain embedded schemas that describe entire
        properties.

        4) labels: A dictionary structure mapping the "path" to every property that is accessible by traversing the
        metadata schema graph structure, initiating from this top-level schema. For example, the "donor_organism" Type
        metadata schema contains the property "biomaterial_core" which is described by its own Core schema and contains
        a property "biomaterial_name". The key that would be added here would be "biomaterial_name" and its value would
        be "donor_organism.biomaterial_core.biomaterial_name".

        5) tabs: A list of dictionaries where each dictionary represents a tab that would be generated in the overall
        spreadsheet if this entire schema was turned into a spreadsheet.

        6) migrations: A nested dictionary describing the migrations that have occurred for each property in any
        metadata schema.

        :param ingest_api_url: A string representing the root Ingest API URL from which the latest metadata schema
                               URLs will be queried.
        :param migrations_url: A string representing the URL containing the property migration file that enumerates
                               changes in metadata schemas between versions.
        :param metadata_schema_urls: A list of strings representing URLs, each containing a JSON-formatted metadata
                                     schema. If this parameter is provided, the parameter json_schema_docs should not be
                                     populated.
        :param json_schema_docs: A list of objects where each object represents a deserialized JSON-formatted
                                 metadata schema. If this parameter is provided, the parameter metadata_schema_urls
                                 should not be populated.
        :param spreadsheet_configuration: A TabConfig object that represents the expected tabs that will be created
                                          in a spreadsheet given the metadata schemas provided.
        :param property_migrations: An object representing a deserialized JSON-string which is a property migrations
                                    file. A property migrations files dictates how the version of the schema has changed
                                    from an older version.
        :param custom_properties: An object representing a deserialized JSON-string which contains custom fields added
                                  on top of metadata schema
        """

        # Function validation: only one of json_schema_docs or metadata_schema_urls may be populated or neither (NAND
        # boolean).
        if bool(metadata_schema_urls) and bool(json_schema_docs):
            raise Exception(
                'Only one of function arguments metadata_schema_urls or json_schema_docs (or neither) may be '
                'populated when initializing SchemaTemplate.')

        self.metadata_schema_urls = metadata_schema_urls
        # If neither the metadata_schema_urls are given nor the json_schema_docs, fetch the metadata schema URLs via
        # querying the Ingest API.
        if not metadata_schema_urls and not json_schema_docs:
            self.metadata_schema_urls = self._get_latest_submittable_schema_urls(ingest_api_url)

        self.property_migrations = property_migrations
        # If the property migrations were not given as input, read the migrations from the migrations URL and store
        # into the deserialized JSON into a Python object.
        if not self.property_migrations:
            self.property_migrations = self._get_property_migrations_json_obj(migrations_url)

        self.json_schemas = json_schema_docs
        if not self.json_schemas:
            self.json_schemas = self._get_json_objs_from_metadata_schema_urls()

        self.template_version = "1.0.0"
        self.created_date = str(datetime.now())
        self.meta_data_properties = {}
        self.custom_properties = {} if not custom_properties else custom_properties
        self.labels = {}
        self.tabs = []
        self.migrations = {}

        for json_schema in self.json_schemas:
            schema_parser = SchemaParser(json_schema)
            schema_descriptor = schema_parser.schema_descriptor
            fully_parsed_dictionary = schema_parser.schema_dictionary
            self.meta_data_properties[schema_descriptor.get_schema_module_name()] = fully_parsed_dictionary
            self.tabs.append(schema_parser.get_tab_representation_of_schema())
            self.labels.update(schema_parser.get_map_of_paths_by_property_label(
                {schema_descriptor.get_schema_module_name(): fully_parsed_dictionary}))
            self.init_custom_properties(schema_descriptor)

        self.migrations = MigrationParser(self.property_migrations).migrations

        self.spreadsheet_configuration = tab_config if tab_config else TabConfig(self.get_dictionary_representation())

    def init_custom_properties(self, schema_descriptor):
        external_field_descriptor = SimplePropertyDescriptor({})
        external_field_descriptor.identifiable = True
        external_field_descriptor.external_reference = True
        self.custom_properties[schema_descriptor.get_schema_module_name()] = {}
        self.custom_properties[schema_descriptor.get_schema_module_name()][
            EXTERNAL_REFERENCE_FIELD] = external_field_descriptor.get_dictionary_representation_of_descriptor()

    def get_dictionary_representation(self):
        return {
            "template_versions": self.template_version,
            "created_date": self.created_date,
            "meta_data_properties": self.meta_data_properties,
            "labels": self.labels,
            "tabs": self.tabs,
            "migrations": MigrationParser.get_dictionary_representation(self.migrations)
        }

    def get_list_of_schema_spreadsheet_representations(self):
        return self.tabs

    def lookup_property_attributes_in_metadata(self, property_key):
        """
        Given a property key which details the full path to the property from the top level schema, returns a dictionary
        representing the attributes of the requested property.

        :param property_key: A string representing the fully qualified path to the desired metadata property
        :return: A dictionary representing the attributes of the property. If none is found, an exception will be thrown
        """

        result = self._lookup_fully_qualified_key_path_in_dictionary(property_key, self.meta_data_properties)

        if not result:
            raise UnknownKeySchemaException(f"ERROR: Cannot find key {property_key} in any schema!")

        return result

    def lookup_property_from_template(self, property_key):
        """
        Given a property key which details the full path to the property from the top level schema, returns a dictionary
        representing the attributes of the requested property.

        :param property_key: A string representing the fully qualified path to the desired metadata or custom property
        :return: A dictionary representing the attributes of the property. If none is found, an exception will be thrown
        """
        result = self._lookup_fully_qualified_key_path_in_dictionary(property_key, self.meta_data_properties)

        if not result:
            result = self._lookup_fully_qualified_key_path_in_dictionary(property_key, self.custom_properties)

        if not result:
            raise UnknownKeySchemaException(f"ERROR: Cannot find key {property_key} in any schema!")

        return result

    def lookup_metadata_schema_name_given_title(self, tab_display_name):
        """
        Given a tab's display name which is often used to label a column in a spreadsheet that represents a property of
        a schema, return the fully qualified path that is the equivalent key (the key can then be used to fetch
        additional attributes about the property).

        :param tab_display_name: A string representing a display name of a schema property.
        :return: A string representing the display name's fully qualified path from a schema.
        """

        try:
            return self.spreadsheet_configuration.get_key_for_label(tab_display_name)
        except KeyError:
            raise UnknownKeySchemaException(
                f"ERROR: Was unable to find a fully qualified path (key) with the display name {tab_display_name}.")

    def lookup_next_latest_key_migration(self, fully_qualified_key):
        """
        Given a key, returns the fully qualified key in the next latest schema (potentially not the absolute latest
        schema though) if it has been replaced.

        :param key: A string representing a fully qualified field name in an older metadata schema version.
        :return: A string representing a fully qualified field name in the next latest metadata schema version if a
                 migration to that schema version exists. If a migration does not exist, returns the original fully
                 qualified key.
        """

        if not self._validate_fully_qualified_key_exists(fully_qualified_key):
            raise Exception(f"ERROR: Fully qualified key {fully_qualified_key} does not exist in any of the schemas.")

        if fully_qualified_key in self.migrations:
            migration_info = self.migrations[fully_qualified_key]
            return migration_info.replaced_by if migration_info.replaced_by in migration_info else fully_qualified_key

        return fully_qualified_key

    def lookup_absolute_latest_key_migration(self, fully_qualified_key):
        """
        Given a key, returns the fully qualified key in the maximal latest schema if it has been replaced.

        :param key: A string representing a fully qualified field name in an older metadata schema version.
        :return: A string representing a fully qualified field name in the latest metadata schema
                 version if a migration chain to that schema version exists. If a migration does not exist, returns the
                 original fully qualified key.
        """

        if not self._validate_fully_qualified_key_exists(fully_qualified_key):
            raise Exception(f"ERROR: Fully qualified key {fully_qualified_key} does not exist in any of the schemas.")

        original_key = fully_qualified_key
        next_latest_migrated_key = self.lookup_next_latest_key_migration(fully_qualified_key)

        while original_key != next_latest_migrated_key:
            original_key = next_latest_migrated_key
            next_latest_migrated_key = self.lookup_next_latest_key_migration(original_key)

        return next_latest_migrated_key

    def generate_yaml_representation_of_spreadsheets(self, tabs_only):
        """
        Generate a YAML object representing the schema_template object and if `tabs_only` is true, then only generate
        the YAML based on self.tabs. This YAML object can be used to generate arbitrary spreadsheets based on the
        metadata schemas.

        :param tabs_only: When true, only uses the tabs to generate the YAML instead of including all the "metadata"
                          about the metadata schemas encapsulated in this SchemaTemplate object.
        :return: A YAML object representing the metadata schemas encapsulated in this SchemaTemplate object.
        """

        yaml.default_flow_style = False
        return yaml.dump({"tabs": self.tabs} if tabs_only else self.get_dictionary_representation(), indent=4)

    def _validate_fully_qualified_key_exists(self, fully_qualified_key):
        return fully_qualified_key in self.labels

    def _lookup_fully_qualified_key_path_in_dictionary(self, fully_defined_metadata_property, dictionary):
        if fully_defined_metadata_property in dictionary:
            return dictionary[fully_defined_metadata_property]

        if '.' in fully_defined_metadata_property:
            expected_schema = fully_defined_metadata_property.split('.')[0]
            if expected_schema in dictionary:
                return self._lookup_fully_qualified_key_path_in_dictionary(
                    fully_defined_metadata_property.split('.', 1)[1], dictionary[expected_schema])

    def _get_latest_submittable_schema_urls(self, ingest_api_url):
        """
        Given the Ingest API URL, send a request to get the latest metadata schemas and return the URLs.

        :param ingest_api_url: A string representing the API URL from which to query for schemas.
        :return: A list of strings where each string represents a URL representing the location of a JSON-formatted
                 metadata schema.
        """

        ingest_api = IngestApi(url=ingest_api_url)
        raw_schemas_from_ingest_api = ingest_api.get_schemas(high_level_entity="type", latest_only=True)
        return [schema["_links"]["json-schema"]["href"] for schema in raw_schemas_from_ingest_api]

    def _get_property_migrations_json_obj(self, migrations_url):
        """
        Returns a a JSON-formatted property migrations file read from the given URL.

        :param migrations_url: A string representing the URL from which to read the JSON-formatted property
                               migrations file.
        :return: An object representing the deserialized JSON-formatted property migrations file.
        """

        try:
            return requests.get(migrations_url).json()["migrations"]
        except Exception:
            raise RootSchemaException(f"Was unable to read the property migrations file from URL {migrations_url}")

    def _get_json_objs_from_metadata_schema_urls(self):
        """
        Return a list of objects that represent deserialized JSON-formatted metadata schemas from the URLs stored in
        self.metadata_schema_urls.

        :return: A list of objects representing deserialized JSON-formatted metadata schemas.
        """
        metadata_schema_objs = []
        for uri in self.metadata_schema_urls:
            try:
                metadata_schema_objs.append(requests.get(url=uri).json())
            except Exception:
                raise RootSchemaException(f"Was unable to read metadata schema JSON at {uri}")
        return metadata_schema_objs
