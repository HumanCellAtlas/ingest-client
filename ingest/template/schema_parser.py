import json
import re

import jsonref

from ingest.utils import doctict
from .exceptions import RootSchemaException


class SchemaParser:
    """ A SchemaParser provides functions for accessing objects in a JSON schema. """

    def __init__(self, template):
        self.ignored_properties = ["describedBy", "schema_version", "schema_type", "provenance"]
        self.required_properties = []

        self.schema_template = template

        # TODO: identifiable should be in the schema - hardcoded here for now.
        self._identifiable = ["biomaterial_id", "process_id", "protocol_id", "file_name"]

        self._key_lookup = {}

    def load_schema(self, json_schema):
        """
        Resolve references in a given JSON-formatted metadata schema and populate a SchemaTemplate object with the
        data in the metadata schema.

        :param json_schema: An object representing a deserialized JSON-formatted metadata schema with references.
        :return: An object representing a deserialized JSON-formatted metadata schema with all its references resolved.
        """

        # Use jsonrefs to resolve all $refs in JSON
        metadata_schema_data = jsonref.loads(json.dumps(json_schema))
        return self.initialise_template(metadata_schema_data)

    def load_migration(self, property_migration):
        return self.initialise_property_migration_template(property_migration)

    def key_lookup(self, key):
        return self._key_lookup[key]

    def initialise_property_migration_template(self, property_migration):
        migrated_property = property_migration["source_schema"] + "." + property_migration["property"]

        migration_info = {}

        if "target_schema" in property_migration and "replaced_by" in property_migration:
            migration_info["replaced_by"] = \
                property_migration["target_schema"] + "." + property_migration["replaced_by"]

        if "effective_from" in property_migration:
            migration_info["version"] = property_migration["effective_from"]
        elif "effective_from_source" in property_migration:
            migration_info["version"] = property_migration["effective_from_source"]
            migration_info["target_version"] = property_migration["effective_from_target"]

        migration_info = {migrated_property.split(".")[-1]: migration_info}
        for part in reversed(migrated_property.split(".")[:-1]):
            migration_info = {part: migration_info}

        self.schema_template.put_migration(migration_info)
        return self.schema_template

    def initialise_template(self, data):

        self.get_required_properties_from_metadata_schema(data)

        property = self._extract_property(data)
        if not property.schema or "type" not in property.schema.high_level_entity:
            raise RootSchemaException(
                "Schema must start with a root submittable type schema")
        else:
            property.uuid = {'external_reference': True, 'identifiable': True}

        # todo get tab display name from schema
        tab_display = property.schema.module[0].upper() + property.schema.module[1:].replace("_", " ")
        tab_info = {property.schema.module: {"display_name": tab_display, "columns": []}}

        self.schema_template.append_tab(tab_info)
        self.schema_template.put(property.schema.module, property)

        self._recursive_fill_properties(property.schema.module, data)

        self.schema_template.set_label_mappings(self._key_lookup)
        return self.schema_template

    def _get_path(self, str1, str2):
        return ".".join([str1, str2.split('/')[0]])

    def _recursive_fill_properties(self, path, data):

        for property_name, property_block in self._get_schema_properties_from_object(data).items():
            new_path = self._get_path(path, property_name)
            property = self._extract_property(property_block, property_name=property_name, key=new_path)
            doctict.put(self.schema_template.get_template(), new_path, property)

            self._recursive_fill_properties(new_path, property_block)

    def get_required_properties_from_metadata_schema(self, data):

        if "required" in data:
            self.required_properties = list(set().union(self.required_properties, data["required"]))

    def create_new_template_for_property(self):
        """ Returns a dictionary populated with keys and respective default values that represent metadata about a
        property that exists in a metadata schema.
        """

        return {
            "multivalue": False,
            "format": None,
            "required": False,
            "identifiable": False,
            "external_reference": False,
            "user_friendly": None,
            "description": None,
            "example": None,
            "guidelines": None,
            "value_type": "string"}

    def _extract_property(self, data, *args, **kwargs):

        property_metadata = self.create_new_template_for_property()

        if "type" in data:
            property_metadata["value_type"] = data["type"]
            if data["type"] == "array":
                items = data.get("items", {})
                property_metadata["value_type"] = items.get('type', 'string')
                property_metadata["multivalue"] = True

        schema = self._get_schema_from_object(data)

        if 'property_name' in kwargs:
            if kwargs.get('property_name') in self.required_properties:
                property_metadata["required"] = True

            if kwargs.get('property_name') in self._identifiable:
                property_metadata["identifiable"] = True

        if 'key' in kwargs and "object" != property_metadata["value_type"]:
            self.schema_template.append_column_to_tab(kwargs.get('key'))

        if schema:
            property_metadata["schema"] = schema

        if 'key' in kwargs:

            self._update_label_to_key_map(kwargs.get("key"), kwargs.get("key"))

            if "user_friendly" in data:
                property_metadata["user_friendly"] = data["user_friendly"]
                self._update_label_to_key_map(data["user_friendly"], kwargs.get("key"))

            elif isinstance(data, jsonref.JsonRef) and "user_friendly" in data.__reference__:
                property_metadata["user_friendly"] = data.__reference__["user_friendly"]
                self._update_label_to_key_map(data.__reference__["user_friendly"], kwargs.get("key"))

        if "description" in data:
            property_metadata["description"] = data["description"]
        elif isinstance(data, jsonref.JsonRef) and "description" in data.__reference__:
            property_metadata["description"] = data.__reference__["description"]

        if "format" in data:
            property_metadata["format"] = data["format"]

        if "example" in data:
            property_metadata["example"] = data["example"]

        if "guidelines" in data:
            property_metadata["guidelines"] = data["guidelines"]

        return doctict.DotDict(property_metadata)

    def _update_label_to_key_map(self, label, key):
        values = []
        if label.lower() not in self._key_lookup:
            values = [key]
        else:
            values = self._key_lookup[label.lower()]
            values.append(key)

        if key not in self._key_lookup:
            self._key_lookup[key] = [key]

        self._key_lookup[label.lower()] = list(set(values))

    def _get_schema_from_object(self, data):
        """ Given a JSON object get the id and work out the high-level metadata about the metadata schema. """
        if "items" in data:
            return self._get_schema_from_object(data["items"])

        url_key = None
        if '$id' in data:
            url_key = '$id'
        if 'id' in data:
            url_key = 'id'

        if url_key:
            url = data[url_key]
            return self.create_and_populate_schema_given_information_in_url(url)
        return None

    def create_and_populate_schema_given_information_in_url(self, url):
        """
        Given a URL, create and populate a Schema with high level information about the schema gleaned directly from
        the URL.

        :param url: A string representing the URL of a metadata schema.
        :return: A Schema object that contains a dictionary with metadata about the metadata schema.
        """

        schema = Schema().build()

        # Populate the high level entity
        pattern = re.compile("http[s]?://[^/]*/([^/]*)/")
        match = pattern.search(url)
        schema.high_level_entity = match.group(1)

        # Populate the domain entity
        pattern = re.compile("http[s]?://[^/]*/[^/]*/(?P<domain_entity>.*)/(((\d+\.)?(\d+\.)?(\*|\d+))|(latest))/.*")
        match = pattern.search(url)
        schema.domain_entity = match.group(1) if match else None

        # Populate the module
        schema.module = url.rsplit('/', 1)[-1]

        # Populate the version
        schema.version = url.rsplit('/', 2)[-2]

        # Populate the url
        schema.url = url

        return schema

    def _get_schema_properties_from_object(self, object):
        self.get_required_properties_from_metadata_schema(object)

        if "items" in object and isinstance(object["items"], dict):
            return self._get_schema_properties_from_object(object["items"])

        if "properties" in object and isinstance(object["properties"], dict):
            keys_to_remove = set(self.ignored_properties).intersection(set(object["properties"].keys()))

            for unwanted_key in keys_to_remove:
                del object["properties"][unwanted_key]
            return object["properties"]
        return {}


class Schema:
    def __init__(self):
        self.dict = {}

    def build(self):
        self.dict = {
            "high_level_entity": None,
            "domain_entity": None,
            "module": None,
            "version": None,
            "url": None,
        }
        return doctict.DotDict(self.dict)
