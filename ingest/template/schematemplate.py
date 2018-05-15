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
from ingest.utils import doctict
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

    schema_template.load_schema(list_of_schema_file)
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
        try:
            return self.get(self.meta_data_properties, key)
        except:
            return None

    def yaml_dump(self):
        return yaml_dump(yaml_load(self.json_dump()))

    def json_dump(self):
        return json.dumps(self.__dict__, indent=4)

    def get_key_for_label(self, label, tab=None, tabs_config=None):

        if tab and tabs_config:
            tab_key = tabs_config.get_key_for_label(tab)

            for key in tabs_config.lookup("tabs."+tab_key+".columns"):
                if key in self.labels[label.lower()]:
                    return key

        raise UnknownKeyException(
            "Can't map the key to a known JSON schema property")



    def get(self, d, keys):
        try:
            if "." in keys:
                key, rest = keys.split(".", 1)
                return self.get(d[key], rest)
            else:
                return d[keys]
        except:
            print("Key error: " +keys)
            raise UnknownKeyException(
                "Can't map the key to a known JSON schema property")

class Error(Exception):
    """Base-class for all exceptions raised by this module."""


class RootSchemaException(Error):
    """When generating a template we have to start with root JSON objects"""

class UnknownKeyException(Error):
    """Can't map the key to a known property"""

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

        endpoint = self.get_core_type_from_url(property.schema.url)

        # self.schema_template.meta_data_properties[endpoint] = {}
        self.schema_template.meta_data_properties[property.schema.module] = property

        # path = self._get_path(endpoint, property.schema.module)
        self._recursive_fill_properties(property.schema.module, data)

        self.schema_template.labels = self.key_lookup
        return self.schema_template

    def _get_path(self, str1, str2):
        return ".".join([str1, str2.split('/')[0]])

    def _recursive_fill_properties(self, path, data):

        for property_name, property_block in self._get_schema_properties_from_object(data).items():
            self._collect_required_properties(property_block)

            new_path =  self._get_path(path, property_name)
            property = self._extract_property(property_block, property_name=property_name, key=new_path)
            doctict.put(self.schema_template.meta_data_properties, new_path, property)
            self._recursive_fill_properties(new_path, property_block)

    def _collect_required_properties(self, data):
        if "required" in data:
            self.required = list(set().union(self.required, data["required"]))

    def _extract_property(self, data, *args, **kwargs):

        dic = {"multivalue": False}

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
            self._update_key_to_label(data["user_friendly"], kwargs)

        if "description" in data:
            dic["description"] = data["description"]

        return doctict.DotDict(dic)

    def _update_key_to_label(self, label, kwargs ):
        values = []
        if 'key' in kwargs:
            if label.lower() not in self.key_lookup:
                values =  [ kwargs.get("key") ]
            else:
                values = self.key_lookup[label.lower()]
                values.append(kwargs.get("key"))

            if kwargs.get("key") not in self.key_lookup:
                self.key_lookup[kwargs.get("key")] = [ kwargs.get("key") ]

            self.key_lookup[label.lower()] = list(set(values))

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

    def get_core_type_from_url(self, url):
        pattern = re.compile("http[s]://[^/]*/type/([^/]*)/.*")
        match = pattern.search(url).group(1)

        if match == "process":
            return match + "es"
        return match + "s"

    def _get_schema_properties_from_object(self, object):

        if "items" in object and isinstance(object["items"], dict):
            return self._get_schema_properties_from_object(object["items"])

        if "properties" in object and isinstance(object["properties"], dict):
            keys_to_remove = set(self.properties_to_ignore).intersection(set(object["properties"].keys()))

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
            "url": None,
        }
        return doctict.DotDict(self.dict)


