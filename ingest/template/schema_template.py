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
from ingest.template.tabs import TabConfig
import json
import jsonref
import re
import urllib.request



class SchemaTemplate:
    """
    A schema template is a simplified view over
    JSON schema for the HCA metadata
    """
    def __init__(self, list_of_schema_urls, tab_config=None):


        self._template = {
            "template_version" : "1.0.0",
            "created_date" : str(datetime.now()),
            "meta_data_properties" : {},
            "labels" : {},
            "tabs": []
        }
        self._parser = SchemaParser(self)

        self._load(list_of_schema_urls)

        self._tab_config  = TabConfig(init=self._template)
        if tab_config:
            # override the default tab config if one is supplied
            self._tab_config = tab_config



    def _load(self, list_of_schema_urls):
        """
        given a list of URLs to JSON schema files
        return a SchemaTemplate object
        """
        for uri in list_of_schema_urls:
            with urllib.request.urlopen(uri) as url:
                data = json.loads(url.read().decode())
                self._parser._load_schema(data)
        return self

    def get_tabs_config(self, ):
        return self._tab_config

    def lookup(self, key):
        try:
            return self.get(self._template["meta_data_properties"], key)
        except:
            raise UnknownKeyException(
                "Can't map the key to a known JSON schema property")
            return None

    def get_template(self):
        return self._template["meta_data_properties"]

    def append_tab(self, tab_info):
        self._template["tabs"].append(tab_info)

    def append_column_to_tab(self, property_key):
        level_one = self._get_level_one(property_key)
        for i, tab in enumerate(self._template["tabs"]):
            if level_one in tab:
                self._template["tabs"][i][level_one]["columns"].append(property_key)


    def put (self, property, value):
        '''
        Add a property to the schema template
        :param property:
        :param value:
        :return: void
        '''
        self._template["meta_data_properties"][property] = value

    def set_label_mappings(self, dict):
        '''
        A dictionary of label to keys mapping
        :param label:
        :param key:
        :return: void
        '''
        self._template["labels"] = dict

    def yaml_dump(self):
        return yaml_dump(yaml_load(self.json_dump()))

    def json_dump(self):
        return json.dumps(self._template, indent=4)

    def get_key_for_label(self, column, tab):

        try:

            tab_key = self._tab_config.get_key_for_label(tab)

            for column_key  in self._parser.key_lookup(column.lower()):
                if tab_key == self._get_level_one(column_key):
                    return column_key
        except:
            raise UnknownKeyException(
                "Can't map the key to a known JSON schema property: "+column)

    def _get_level_one(self, key):
        return key.split('.')[0]

    def get(self, d, keys):
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.get(d[key], rest)
        else:
            return d[keys]


class SchemaParser:
    """A schema parser provides functions for
    accessing objects in a JSON schema"""
    def __init__(self, template):

        # always ignore these
        self.properties_to_ignore = \
            ["describedBy", "schema_version", "schema_type"]

        self.schema_template = template

        self._required = []

        # todo identifiable should be in the schema - hard coded here for now
        self._identifiable = ["biomaterial_id", "process_id", "protocol_id", "file_name"]

        self._key_lookup = {}

    def _load_schema(self, json_schema):
        """load a JSON schema representation"""
        # use jsonrefs to resolve all $refs in json
        data = jsonref.loads(json.dumps(json_schema))
        return self.__initialise_template(data)

    def key_lookup(self, key):
        return self._key_lookup[key]

    def __initialise_template(self, data):

        self._collect_required_properties(data)

        property = self._extract_property(data)
        if "type" not in property.schema.high_level_entity:
            raise RootSchemaException(
                "Schema must start with a root submittable type schema")

        # todo get tab display name from schema
        tab_display = property.schema.module[0].upper() + property.schema.module[1:].replace("_", " ")
        tab_info = {property.schema.module : {"display_name": tab_display, "columns" : []}}

        self.schema_template.append_tab(tab_info)
        self.schema_template.put(property.schema.module, property)

        self._recursive_fill_properties(property.schema.module, data)

        self.schema_template.set_label_mappings(self._key_lookup)
        return self.schema_template

    def _get_path(self, str1, str2):
        return ".".join([str1, str2.split('/')[0]])

    def _recursive_fill_properties(self, path, data):

        for property_name, property_block in self._get_schema_properties_from_object(data).items():
            self._collect_required_properties(property_block)

            new_path =  self._get_path(path, property_name)
            property = self._extract_property(property_block, property_name=property_name, key=new_path)
            doctict.put(self.schema_template.get_template(), new_path, property)
            self._recursive_fill_properties(new_path, property_block)

    def _collect_required_properties(self, data):
        if "required" in data:
            self._required = list(set().union(self._required, data["required"]))

    def _extract_property(self, data, *args, **kwargs):

        dic = {"multivalue": False, "required" : False, "user_friendly" : None, "description": None, "example" : None, "value_type": "string"}

        if "type" in data:
            dic["value_type"] = data["type"]
            if data["type"] == "array":
                dic["value_type"] = data["items"]["type"]
                dic["multivalue"] = True

        schema = self._get_schema_from_object(data)

        if 'property_name' in kwargs:
            if kwargs.get('property_name') in self._required:
                dic["required"] = True

            if kwargs.get('property_name') in self._identifiable:
                dic["identifiable"] = True


        if 'key' in kwargs and "object" != dic["value_type"]:
            self.schema_template.append_column_to_tab(kwargs.get('key'))

        if schema:
            dic["schema"] = schema

        if "user_friendly" in data:
            dic["user_friendly"] = data["user_friendly"]
            self._update_key_to_label(data["user_friendly"], kwargs)


        if "description" in data:
            dic["description"] = data["description"]

        if "example" in data:
            dic["example"] = data["example"]

        return doctict.DotDict(dic)

    def _update_key_to_label(self, label, kwargs ):
        values = []
        if 'key' in kwargs:
            if label.lower() not in self._key_lookup:
                values =  [ kwargs.get("key") ]
            else:
                values = self._key_lookup[label.lower()]
                values.append(kwargs.get("key"))

            if kwargs.get("key") not in self._key_lookup:
                self._key_lookup[kwargs.get("key")] = [kwargs.get("key")]

            self._key_lookup[label.lower()] = list(set(values))

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


class Error(Exception):
    """Base-class for all exceptions raised by this module."""


class RootSchemaException(Error):
    """When generating a template we have to start with root JSON objects"""

class UnknownKeyException(Error):
    """Can't map the key to a known property"""

if __name__ == '__main__':
    pass