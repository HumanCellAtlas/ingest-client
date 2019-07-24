#!/usr/bin/env python
"""
This package will return a SchemaTemplate objects from a set of JSON schema files.
"""

import json
import urllib.request
from collections import defaultdict
from datetime import datetime
from itertools import chain

from yaml import dump as yaml_dump, load as yaml_load

from ingest.api.ingestapi import IngestApi
from ingest.template.tabs import TabConfig
from .exceptions import RootSchemaException, UnknownKeySchemaException
from .schema_parser import SchemaParser


class SchemaTemplate:
    """ A SchemaTemplate is data structure representing a simple view of a JSON HCA metadata schema. """

    def __init__(self, ingest_api_url="http://api.ingest.dev.data.humancellatlas.org",
                 migrations_url="https://schema.dev.data.humancellatlas.org/property_migrations",
                 metadata_schema_urls=[], json_schema_docs=[], tab_config=None, property_migrations=None):
        """
        Initialize a set of SchemaTemplate objects where one SchemaTemplate is a representation of a single metadata
        schema file and its associated property migrations.

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
            raise TypeError(
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
            self.property_migrations = self.get_migrations(migrations_url)

        self.json_schemas = json_schema_docs
        if not self.json_schemas:
            self.json_schemas = self.get_json_objs_from_metadata_schema_urls()

        # Create the base template
        self.template = {
            "template_version": "1.0.0",
            "created_date": str(datetime.now()),
            "meta_data_properties": {},
            "labels": {},
            "tabs": [],
            "migrations": {}
        }

        # Add the metadata schemas and the supplementary property migrations to the base template via the SchemaParser.
        self.parser = SchemaParser(self)
        self.populate_schema_from_metadata_schema_and_property_migrations()

        # If a tab configuration is supplied, use that. Otherwise, use one that based off of the template above.
        self.internal_tab_config = tab_config if tab_config else TabConfig(init=self.template)

    def get_schema_urls(self):
        """ Returns a list of metadata schema urls that have been used to instantiate the SchemaTemplate. """
        return self.metadata_schema_urls

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

    def get_migrations(self, migrations_url):
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

    def get_json_objs_from_metadata_schema_urls(self):
        """
        Return a list of objects that represent deserialized JSON-formatted metadata schemas from the URLs stored in
        self.metadata_schema_urls.

        :return: A list of objects representing deserialized JSON-formatted metadata schemas.
        """
        metadata_schema_objs = []
        for uri in self.metadata_schema_urls:
            try:
                with urllib.request.urlopen(uri) as url:
                    metadata_schema_objs.append(json.loads(url.read().decode()))
            except Exception:
                raise RootSchemaException(f"Was unable to read metadata schema JSON at {uri}")
        return metadata_schema_objs

    def populate_schema_from_metadata_schema_and_property_migrations(self):
        """ Parse and load the metadata schemas and respective property migrations into the schema via the
        SchemaParser.
        """
        [self.parser.load_schema(metadata_schema) for metadata_schema in self.json_schemas]
        [self.parser.load_migration(migration) for migration in self.property_migrations]

    @property
    def tab_config(self):
        return self.internal_tab_config

    @tab_config.setter
    def tab_config(self, value):
        self.internal_tab_config = value

    def lookup(self, key):
        try:
            return self.get(self.template["meta_data_properties"], key)
        except Exception:
            raise UnknownKeySchemaException(
                "Can't map the key to a known JSON schema property: " + str(key))

    def replaced_by(self, key):
        """
        Given a key, returns the fully qualified key in the next latest schema (potentially not the absolute latest
        schema though) if it has been replaced.

        :param key: A string representing a fully qualified field name in an older metadata schema version.
        :return: A string representing a fully qualified field name in the next latest metadata schema version if a
                 migration to that schema version exists.
        """
        try:
            field_name = ""
            if key.split(".")[-1] in self.parser.create_new_template_for_property().keys():
                field_name = "." + key.split(".")[-1]
                key = ".".join(key.split(".")[:-1])

            return (self._lookup_migration(key)) + field_name
        except Exception:
            raise UnknownKeySchemaException(
                "Can't map the key to a known JSON schema property: " + str(key))

    def replaced_by_latest(self, key):
        """
        Tells you the latest fully qualified key if it has been replaced. This function will check that it is also
        valid in the latest loaded schema.

        :param key: A string representing a fully qualified field name in an older metadata schema version.
        :return: A string representing a fully qualified field name in the latest metadata schema version if it exists.
        """
        replaced_by = self._lookup_migration(key)

        try:
            self.lookup(replaced_by)
            return replaced_by
        except UnknownKeySchemaException:
            if key == replaced_by:
                raise UnknownKeySchemaException(
                    "Can't map the key to a known JSON schema property: " + str(key))
            return self.replaced_by_latest(replaced_by)

    def replaced_by_at(self, key, schema_version):
        """
        Tells you if the fully qualified key has been replaced at this version. This method will not check if it is a
        valid key in the schema, it only tells you if it has been replaced. If it hasn't been replaced it will return
        the key.

        :param key: A string representing a fully qualified field name in an older metadata schema version.
        :param schema_version: A string representing the schema version at which to check whether the key has been
                               replaced with an equivalent and different fully qualified name.
        :return: A string representing a fully qualified field name at the specified schema version if it exists.
        """
        try:
            replaced_by = self._lookup_migration(key)
            if key == replaced_by:
                return key

            version = self._lookup_migration_version(key)

            if int(schema_version.split(".")[0]) < int(version.split(".")[0]):
                # the requested schema version is before the migration so
                # just return the original key
                return key

            next_replaced_by_version = self._lookup_migration_version(replaced_by) or schema_version
            if version == next_replaced_by_version:
                next_replaced_by_version = schema_version

            if int(version.split(".")[0]) \
                    <= int(schema_version.split(".")[0]) \
                    <= int(next_replaced_by_version.split(".")[0]):
                return replaced_by
            else:
                return self.replaced_by_at(replaced_by, schema_version)
        except Exception:
            raise UnknownKeySchemaException(
                "Can't map the key to a known JSON schema property: " + str(key))

    def _lookup_migration(self, key):
        migration, backtrack = self._find_migration_object(key)

        if "replaced_by" in migration:
            if (backtrack):
                return migration["replaced_by"] + backtrack
            return migration["replaced_by"]
        else:
            return key

    def _lookup_migration_version(self, key):
        migration, backtrack = self._find_migration_object(key)

        if "version" in migration:
            return migration["version"]
        return None

    def _find_migration_object(self, fq_key):
        backtrack_fq_key = ""
        while True:
            try:
                migration_object = self.get(self.template["migrations"], fq_key)
                return migration_object, backtrack_fq_key
            except Exception:
                fq_key = fq_key.split(".")
                backtrack_fq_key = "." + fq_key.pop()
                fq_key = ".".join(fq_key)
                if "." not in fq_key:
                    break
        return {}, backtrack_fq_key

    def get_template(self):
        return self.template["meta_data_properties"]

    def append_tab(self, tab_info):
        self.template["tabs"].append(tab_info)

    def append_column_to_tab(self, property_key):
        level_one = self._get_level_one(property_key)
        for i, tab in enumerate(self.template["tabs"]):
            if level_one in tab:
                self.template["tabs"][i][level_one]["columns"].append(property_key)

    def put_migration(self, property_migration):
        for k, v in property_migration.items():
            if k in self.template["migrations"]:
                self.template["migrations"][k] = self._mergeDict(self.template["migrations"][k], v)
            else:
                self.template["migrations"][k] = v

    def _mergeDict(self, dict1, dict2):
        dict3 = defaultdict(list)
        for k, v in chain(dict1.items(), dict2.items()):
            if k in dict3:
                if isinstance(v, dict):
                    dict3[k].update(self._mergeDict(dict3[k], v))
                elif isinstance(v, list) and isinstance(dict3[k], list) and len(v) == len(dict3[k]):
                    for index, e in enumerate(v):
                        dict3[k][index].update(self._mergeDict(dict3[k][index], e))
                else:
                    dict3[k].update(v)
            else:
                dict3[k] = v
        return dict3

    def put(self, property, value):
        """ Add a property to the schema template """
        self.template["meta_data_properties"][property] = value

    def set_label_mappings(self, dict):
        """ A dictionary of label to keys mapping """
        self.template["labels"] = dict

    def yaml_dump(self, tabs_only=False):
        return yaml_dump(yaml_load(self.json_dump(tabs_only)), default_flow_style=False)

    def json_dump(self, tabs_only=False):
        if tabs_only:
            tabs = {"tabs": self.template["tabs"]}
            return json.dumps(tabs, indent=4)
        return json.dumps(self.template, indent=4)

    def get_key_for_label(self, column, tab):
        try:
            tab_key = self.tab_config.get_key_for_label(tab)
            for column_key in self.parser.key_lookup(column.lower()):
                if tab_key == self._get_level_one(column_key):
                    return column_key
        except Exception:
            raise UnknownKeySchemaException(
                "Can't map the key to a known JSON schema property: " + str(column))

    def get_tab_key(self, label):
        try:
            return self.tab_config.get_key_for_label(label)
        except KeyError:
            raise UnknownKeySchemaException(f'No key found for [{label}].')

    def _get_level_one(self, key):
        return key.split('.')[0]

    def get(self, d, keys):
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.get(d[key], rest)
        else:
            return d[keys]
