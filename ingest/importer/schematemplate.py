#!/usr/bin/env python
"""
This package will return a SchemaTemplate objects
from a set of JSON schema files.
"""

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "01/05/2018"

from datetime import datetime
from yaml import dump as yaml_dump
from yaml import load as yaml_load

import json
import jsonref
import re
import urllib.request


def get_template_from_schemas_by_url(list_of_schema_urls):
    """
    given a list of URLs to JSON schema files
    return a SchemaTemplate object
    """
    parser = SchemaParser()
    for uri in list_of_schema_urls:
        with urllib.request.urlopen(uri) as url:
            data = json.loads(url.read().decode())
            parser.load_schema(data)
    return parser.schema_template


def get_template_from_schemas_by_file(list_of_schema_file):
    """
    given a list of JSON schema files
    return a SchemaTemplate object
    """
    return None


def get_template_from_schemas(list_of_schema_file):
    """
    given a list of JSON schema objects
    return a SchemaTemplate object
    """
    schema_template = SchemaParser()

    # for each file

    # check is valid json schema

    # add json schema

    schema_template.load_schema()
    return None


class SchemaTemplate:
    """
    A schema template is a simplified view over
    JSON schema for the HCA metadata
    """
    def __init__(self):

        self.template_version = "1.0.0"
        self.created_date = str(datetime.now())
        self.meta_data_properties = {}
        self.tabs = {}
        self.labels = {}

    def lookup(self, key):
        return self.get(self.meta_data_properties, key)

    def yaml_dump(self):
        return yaml_dump(yaml_load(self.json_dump()))

    def json_dump(self):
        return json.dumps(self.__dict__, indent=4)

    def get_key_for_label(self, label):
        return self.labels[label.lower()]

    def get(self, d, keys):
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.get(d[key], rest)
        else:
            return d[keys]


class Error(Exception):
    """Base-class for all exceptions raised by this module."""


class RootSchemaException(Error):
    """When generating a template we have to start with root JSON objects"""


class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class SchemaParser:
    """A schema parser provides functions for
    accessing objects in a JSON schema"""
    def __init__(self):
        self.properties_to_ignore = \
            ["describedBy", "schema_version", "schema_type"]
        self.schema_template = SchemaTemplate()
        self.required = []
        self.key_lookup = {}

    def load_schema(self, json_schema):
        """load a JSON schema representation"""
        # use jsonrefs to resolve all $refs in json
        data = jsonref.loads(json.dumps(json_schema))
        return self.__initialise_template(data)

    def __initialise_template(self, data):

        self._collect_required_properties(data)

        property = self._extract_property(data)
        if "type" not in property.schema.high_level_entity:
            raise RootSchemaException(
                "Schema must start with a root submittable type schema")

        endpoint = get_endpoint_name_for_schema(property.schema)

        self.schema_template.meta_data_properties[endpoint] = {}
        self.schema_template.meta_data_properties[endpoint][property.schema.domain_entity] = property

        path = endpoint + "." + property.schema.domain_entity
        self._recursive_fill_properties(path, data)

        self.schema_template.labels = self.key_lookup
        return self.schema_template

    def put(self, d, keys, item):
        if "." in keys:
            key, rest = keys.split(".", 1)
            if key not in d:
                d[key] = {}
            self.put(d[key], rest, item)
        else:
            d[keys] = item

    def get(self, d, keys):
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.get(d[key], rest)
        else:
            return d[keys]

    def _recursive_fill_properties(self, path, data):

        for property_name, property_block in self._get_schema_properties_from_object(data).items():
            self._collect_required_properties(property_block)

            new_path = path + "." + property_name
            property = self._extract_property(property_block, property_name=property_name, key=new_path)
            self.put(self.schema_template.meta_data_properties, new_path, property)
            self._recursive_fill_properties(new_path, property_block)

    def _collect_required_properties(self, data):
        if "required" in data:
            self.required = list(set().union(self.required, data["required"]))

    def _extract_property(self, data, *args, **kwargs):

        dic = {
            "multivalue": False,
            "links_to": [],
            "values": [],
            "ontology_values": {}
        }

        if "type" in data:
            dic["value_type"] = data["type"]
            if data["type"] == "array":
                dic["value_type"] = data["items"]["type"]
                dic["multivalue"] = True

        schema = self._get_schema_from_object(data)

        if 'property_name' in kwargs and kwargs.get('property_name') in self.required:
            dic["required"] = True

        if schema:
            dic["schema"] = schema

        if "user_friendly" in data:
            dic["user_friendly"] = data["user_friendly"]
            if 'key' in kwargs:
                self.key_lookup[data["user_friendly"].lower()] = kwargs.get("key")

        if "description" in data:
            dic["description"] = data["description"]

        return dotdict(dic)

    def _get_schema_from_object(self, data):
        """
        given a JSON object get the id and work out the
        """
        if "items" in data:
            return self._get_schema_from_object(data["items"])

        if "id" in data:
            url = data["id"]
            schema = Schema().build()
            schema.domain_entity = self.get_domain_entity_from_url(url)
            schema.high_level_entity = self.get_high_level_entity_from_url(url)
            schema.module = self.get_module_from_url(url)
            schema.url = url
            return schema
        return None

    def get_high_level_entity_from_url(self, url):
        pattern = re.compile("http[s]://[^/]*/([^/]*)/")
        match = pattern.search(url)
        return match.group(1)

    def get_domain_entity_from_url(self, url):
        pattern = re.compile("http[s]://[^/]*/[^/]*/(.*)/(\d+\.)?(\d+\.)?(\*|\d+)/.*")
        match = pattern.search(url)
        return match.group(1)

    def get_module_from_url(self, url):
        return url.rsplit('/', 1)[-1]

    def _get_schema_properties_from_object(self, object):

        if "items" in object and isinstance(object["items"], dict):
            return self._get_schema_properties_from_object(object["items"])

        if "properties" in object and isinstance(object["properties"], dict):
            keys_to_remove = set(self.properties_to_ignore).intersection(set(object["properties"].keys()))

            for unwanted_key in keys_to_remove:
                del object["properties"][unwanted_key]
            return object["properties"]
        return {}


def get_endpoint_name_for_schema(schema):
    if schema.domain_entity == "process":
        return schema.domain_entity + "es"
    return schema.domain_entity + "s"


class Schema:
    def __init__(self):
        self.dict = {}

    def get_endpoint(self, schema):
        if schema.domain_entity == "process":
            return schema.domain_entity + "es"
        return schema.domain_entity + "s"

    def build(self):
        self.dict = {
            "high_level_entity": None,
            "domain_entity": None,
            "module": None,
            "url": None,
            "required_fields": []
        }
        return dotdict(self.dict)


class Tab:
    def __init__(self):
        self.display_name = ""
        self.columns = []
