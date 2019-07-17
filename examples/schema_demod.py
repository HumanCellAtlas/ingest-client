#!/usr/bin/env python
"""
Script to demodularise the HCA metadta schemas by resolvong all $refs
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "11/01/2019"


from ingest.template.schema_template import SchemaTemplate
from ingest.template.schema_template import SchemaParser
from yaml import dump as yaml_dump
from yaml import load as yaml_load

import urllib.request
import json
import jsonref

template = SchemaTemplate()
parser = SchemaParser(template)

INGESTAPI = "http://api.ingest.dev.data.humancellatlas.org"

list_of_schema_urls = template.get_latest_submittable_schema_urls(INGESTAPI)

def get_data(uri):

    print("getting " + uri)

    with urllib.request.urlopen(uri) as url:
        data = json.loads(url.read().decode())

        if parser.get_high_level_entity_from_url(uri) != 'type':
            del data["$id"]

            del data["$schema"]
            if "additionalProperties" in data:
                del data["additionalProperties"]

            del data["properties"]["describedBy"]

            if "schema_version" in data["properties"]:
                del data["properties"]["schema_version"]

        return data

for uri in list_of_schema_urls:

    with urllib.request.urlopen(uri) as url:

        data = json.loads(url.read().decode())

        demod_schema = jsonref.loads(json.dumps(data), loader=get_data)

        schema_name = uri.rsplit('/', 1)[-1]

        domain = parser.get_domain_entity_from_url(uri).rsplit('/', 1)[0]

        with open(domain+'/'+schema_name+'.yaml', 'w') as outfile:
            json.dump(demod_schema, outfile, indent=4)
