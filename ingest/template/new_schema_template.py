import json
import urllib
from datetime import datetime

import requests

from ingest.api.ingestapi import IngestApi
from .exceptions import RootSchemaException, UnknownKeySchemaException
from .migration_dictionary import MigrationDictionary
from .new_schema_parser import NewSchemaParser


class NewSchemaTemplate():
    """ A SchemaTemplate is used to encapsulate information about all the metadata schema files and
    property migration files that are directly passed in in order to generate a spreadsheet. """

    def __init__(self, ingest_api_url="http://api.ingest.dev.data.humancellatlas.org",
                 migrations_url="https://schema.dev.data.humancellatlas.org/property_migrations",
                 metadata_schema_urls=None, json_schema_docs=None, tab_config=None, property_migrations=None):
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
        :param tab_config: A TabConfig object that represents the expected tabs that will be created in a spreadsheet
                           given the metadata schemas provided.
        :param property_migrations: An object representing a deserialized JSON-string which is a property migrations
                                    file. A property migrations files dictates how the version of the schema has changed
                                    from an older version.
        """

        # Function validation
        # 1) Only one of json_schema_docs or metadata_schema_urls may be populated or neither (NAND boolean).
        if bool(metadata_schema_urls) and bool(json_schema_docs):
            raise Exception(
                'Only one of function arguments metadata_schema_urls or json_schema_docs (or neither) may be '
                'populated when initializing SchemaTemplate.')

        self.metadata_schema_urls = metadata_schema_urls
        # If neither the metadata_schema_urls are given nor the json_schema_docs, fetch the metadata schema URLs via
        # querying the Ingest API.
        if not metadata_schema_urls and not json_schema_docs:
            self.metadata_schema_urls = self.get_latest_submittable_schema_urls(ingest_api_url)

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
        self.labels = {}
        self.tabs = []
        self.migrations = {}

        for json_schema in self.json_schemas:
            schema_parser = NewSchemaParser(json_schema)
            schema_descriptor = schema_parser.schema_descriptor
            fully_parsed_dictionary = schema_parser.schema_dictionary
            self.meta_data_properties[schema_descriptor.get_schema_module_name()] = fully_parsed_dictionary
            self.tabs.append(schema_parser.get_tab_representation_of_schema())
            self.labels.update(schema_parser.get_map_of_paths_by_property_label(
                {schema_descriptor.get_schema_module_name(): fully_parsed_dictionary}))

        self.migrations = self._get_migrations_dictionary()

    def get_dictionary_representation(self):
        return {
            "template_versions": self.template_version,
            "created_date": self.created_date,
            "meta_data_properties": self.meta_data_properties,
            "labels": self.labels,
            "tabs": self.tabs,
            "migrations": self.migrations
        }

    def lookup_property_attributes_in_metadata(self, property_key):
        """
        Given a property key which details the full path to the property from the top level schema,
        return a dictionary representing the attributes of said property.

        :param property_key: A string representing the fully qualified path to the desired metadata property
        :return: A dictionary representing the attributes of the property. If none is found, an exception will be
        thrown.
        """

        return self._lookup_fully_qualified_key_path_in_dictionary(property_key, self.meta_data_properties)

    def _lookup_fully_qualified_key_path_in_dictionary(self, fully_defined_metadata_property, dictionary):
        if fully_defined_metadata_property in dictionary:
            return dictionary[fully_defined_metadata_property]

        if '.' in fully_defined_metadata_property:
            expected_schema = fully_defined_metadata_property.split('.')[0]
            if expected_schema in dictionary:
                return self._lookup_fully_qualified_key_path_in_dictionary(
                    fully_defined_metadata_property.split('.', 1)[1], dictionary[expected_schema])

        raise UnknownKeySchemaException(f"Cannot find key {fully_defined_metadata_property} in any schema!")

    def _lookup_value_in_given_dictionary(self, key, dictionary):
        pass

    def get_latest_submittable_schema_urls(self, ingest_api_url):
        """
        Given the Ingest API URL, send a request to get the latest metadata schemas and return the URLs.

        :param ingest_api_url: A string representing the API URL from which to query for schemas.
        :return: A list of strings where each string represents a URL representing the location of a JSON-formatted
                 metadata schema.
        """

        ingest_api = IngestApi(url=ingest_api_url)
        raw_schemas_from_ingest_api = ingest_api.get_schemas(high_level_entity="type", latest_only=True)
        return [schema["_links"]["json-schema"]["href"] for schema in raw_schemas_from_ingest_api]

    def _get_migrations_dictionary(self):
        migration_dictionary = MigrationDictionary()
        for property_migration in self.property_migrations:
            source_migrated_property = property_migration["source_schema"] + "." + property_migration[
                "property"]

            migration_info = {}

            if "target_schema" in property_migration and "replaced_by" in property_migration:
                migration_info["replaced_by"] = property_migration["target_schema"] + "." + property_migration[
                    "replaced_by"]
            if "effective_from" in property_migration:
                migration_info["version"] = property_migration["effective_from"]
            elif "effective_from_source" in property_migration:
                migration_info["version"] = property_migration["effective_from_source"]
                migration_info["target_version"] = property_migration["effective_from_target"]

            migration_dictionary.put(source_migrated_property, migration_info)
        return migration_dictionary.to_dict()

    def _get_property_migrations_json_obj(self, migrations_url):
        """
        Returns a a JSON-formatted property migrations file read from the given URL.

        :param migrations_url: A string representing the URL from which to read the JSON-formatted property
                               migrations file.
        :return: An object representing the deserialized JSON-formatted property migrations file.
        """

        try:
            with urllib.request.urlopen(migrations_url) as url:
                return json.loads(url.read().decode())["migrations"]
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
