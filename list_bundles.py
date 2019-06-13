#!/usr/bin/env python

"""
python list_bundles.py prod b6dc9b93-929a-45d0-beb2-5cf8e64872fe
python list_bundles.py staging 3b41f062-621c-46ca-abad-bce09427934d
"""

import argparse
import json
import logging
import sys

from ingest.api.ingestapi import IngestApi

logging.getLogger('ingest').setLevel(logging.DEBUG)
format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
         '%(lineno)s %(funcName)s(): %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.WARNING, format=format)


class BundleManifest:
    def __init__(self, resource):
        self._object = resource

    @property
    def fqid(self):
        uuid = self._object.get('bundleUuid')
        version = self._object.get('bundleVersion')
        return f'{uuid}.{version}'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generates a bundle fqid list given a project uuid')
    parser.add_argument('env', choices=['dev', 'integration', 'staging', 'prod'], help='environment')
    parser.add_argument('project_uuid', metavar='project-uuid', type=str, help='Project uuid')
    parser.add_argument('--filename', type=str, help='Output filename')
    args = parser.parse_args()

    project_uuid = args.project_uuid
    filename = args.filename or f'{args.project_uuid}.json'
    env = args.env

    infix = f'.{env}' if env != 'prod' else ''
    url = f'https://api.ingest{infix}.data.humancellatlas.org'
    ingest_api = IngestApi(url)
    project = ingest_api.get_project_by_uuid(project_uuid)
    bundle_manifests = ingest_api.get_related_entities("bundles", project, "bundleManifests")

    bundle_fqids = [BundleManifest(obj).fqid for obj in bundle_manifests]

    with open(filename, 'w') as outfile:
        json.dump(bundle_fqids, outfile, indent=4)

    print(f'Total bundle count: {len(bundle_fqids)}')
    print(f'Saved into file: {filename}')
