#!/usr/bin/env python
"""
This package will return a SchemaTemplate objects from a set of JSON schema files.
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "01/05/2018"

from datetime import datetime
from pprint import pprint

import json
import jsonref

"""
given a list of URLs to JSON schema files
return a SchemaTemplate object
"""
def get_template_from_schemas_by_url(list_of_schema_urls):
    return None

"""
given a list of JSON schema files
return a SchemaTemplate object
"""
def get_template_from_schemas_by_file(list_of_schema_file):
    return None

"""
given a list of JSON schema objects
return a SchemaTemplate object
"""
def get_template_from_schemas(list_of_schema_file):

    schema_template = SchemaTemplate()

    # for each file

    # check is valid json schema

    # add json schema

    schema_template.parse_json()


    return None


"""
A schema template is a simplified view over JSON schema that is used to build and 
"""
class SchemaTemplate:
    def __init__(self):

        self.template_version = "1.0.0"
        self.created_date = datetime.now()
        self.meta_data_properties = []
        self.tabs = []


    """
    load a JSON schema representation
    """
    def load_schema(self, json_schema):
        data = jsonref.loads(json.dumps(json_schema))

        # for each property recurse down the hierarchy creating the meta_data_property


        for key, val in data["properties"].items():
            print ("{}".format(key))



        return self

    def _get_schema_properties_from_object(self, object):

        if "properties" in object and isinstance(object["properties"],dict):
            return object["properties"].keys()
        return []


