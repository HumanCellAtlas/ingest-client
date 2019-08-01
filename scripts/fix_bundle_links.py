import argparse
import json
import logging
import multiprocessing
import sys
import time
from copy import deepcopy

import requests
from hca.util import SwaggerAPIException

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi
from ingest.exporter.ingestexportservice import LinkSet

logging.getLogger('ingest').setLevel(logging.DEBUG)
format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
         '%(lineno)s %(funcName)s(): %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.WARNING, format=format)

PROCESS_COUNT = 15


class Bundle:
    def __init__(self, dss_api, uuid, version):
        self.dss_api = dss_api
        self._object = self.dss_api.get_bundle(uuid, version).get('bundle')
        self._files_map = {file['name']: file for file in self._object.get('files')}

    @property
    def links(self):
        links_file = self._files_map['links.json']
        links_file_uuid = links_file['uuid']
        links_file_version = links_file['version']
        links_file_json = self.dss_api.get_file(links_file_uuid, links_file_version)
        return links_file_json.get('links')


class BundleManifest:
    def __init__(self, dss_api: DssApi, project_uuid, resource):
        self._object = resource
        self.dss_api = dss_api
        self.project_uuid = project_uuid
        self.bundle = None

    @property
    def fqid(self):
        return f'{self.uuid}.{self.version}'

    @property
    def uuid(self):
        return self._object.get('bundleUuid')

    @property
    def version(self):
        return self._object.get('bundleVersion')

    @property
    def url(self):
        return self._object['_links']['self']['href']

    @property
    def links(self):
        if not self._object.get('links'):
            return self.bundle.links
        return self._object.get('links')

    def exists(self):
        try:
            bundle = Bundle(self.dss_api, self.uuid, self.version)
            self.bundle = bundle
        except SwaggerAPIException as e:
            error_code = e.details.get('code')
            if error_code.lower() == 'not_found':
                return False
        return True

    def _is_supplementary_file(self, file_uuid):
        file_json = self.dss_api.get_file(file_uuid)
        described_by = file_json.get('describedBy')
        schema_type = described_by.rsplit('/', 1)[-1]
        return schema_type == 'supplementary_file'

    def _correct_link(self, link):
        input_type = link.get('input_type')
        output_type = link.get('output_type')
        inputs = link.get('inputs')

        if input_type == 'file' and output_type == 'file':
            new_inputs = list(filter(
                lambda file_uuid: not self._is_supplementary_file(file_uuid),
                inputs))
            new_link = deepcopy(link)
            new_link['inputs'] = new_inputs
            return new_link
        return link

    def correct_links(self):
        new_links_set = LinkSet()

        for link in self.links:
            new_links_set.add_link(self._correct_link(link))

        return new_links_set.get_links()


class BundleManifestService:
    def __init__(self, ingest_api: IngestApi, dss_api: DssApi ):
        self.ingest_api = ingest_api
        self.dss_api = dss_api

    def find_bundle_manifests(self, project_uuid):
        project = self.ingest_api.get_project_by_uuid(project_uuid)
        return self.ingest_api.get_related_entities("bundleManifests", project, "bundleManifests")

    def find_bundle_manifests_count(self, project_uuid):
        project = self.ingest_api.get_project_by_uuid(project_uuid)
        return self.ingest_api.get_related_entities_count("bundleManifests", project, "bundleManifests")

    def update_bundle_manifest_links(self, project_uuid):
        bundle_manifests = self.find_bundle_manifests(project_uuid)
        count = 0
        for resource in bundle_manifests:
            bundle_manifest = BundleManifest(self.dss_api, project_uuid, resource)
            bundle_uuid = bundle_manifest.uuid
            version = bundle_manifest.version

            bundle = Bundle(dss_api, bundle_uuid, version)
            self.ingest_api.patch(bundle_manifest.url, {'links': bundle.links})
            count = count + 1

        return {'bundle_manifests_updated_count': count}

    def find_bundles_with_links_correction(self, project_uuid):
        bundles_with_duplicate_links = []
        bundle_manifests = self.find_bundle_manifests(project_uuid)
        reports = {
            'bundles': {},
            'summary': {}
        }
        count = self.find_bundle_manifests_count(project_uuid)
        print(f'Found {count} bundles for project {project_uuid}')
        for i, resource in enumerate(bundle_manifests):
            bundle_manifest = BundleManifest(self.dss_api, project_uuid, resource)
            print(f'Processing {i + 1}/{count} , bundle {bundle_manifest.fqid} ...')

            links = bundle_manifest.correct_links()
            if links != bundle_manifest.links:
                reports['bundles'][bundle_manifest.fqid] = {}
                reports['bundles'][bundle_manifest.fqid]['new'] = links
                reports['bundles'][bundle_manifest.fqid]['old'] = bundle_manifest.links

        reports['summary']['error_count'] = len(list(reports['bundles'].keys()))
        return reports

    def process_bundle_manifest_resource(self, project_uuid, bundle_manifest_resource):
        bundle_manifest = BundleManifest(self.dss_api, project_uuid, bundle_manifest_resource)
        return self.process_bundle_manifest(bundle_manifest)

    def process_project(self, project_uuid):
        bundle_manifest_resources = self.find_bundle_manifests(project_uuid)
        thread_pool = multiprocessing.Pool(20)
        map_results = thread_pool.map(lambda bundle_manifest_resource: bundle_manifest_service.process_bundle_manifest_resource(
            project_uuid, bundle_manifest_resource), bundle_manifest_resources)
        summary = get_summary(map_results)
        save_json_to_file(summary, f'{project_uuid}_summary.log')

    def process_bundle_manifest(self, bundle_manifest: BundleManifest):
        print(f'Processing {bundle_manifest.fqid}')

        if not bundle_manifest.exists():
            return {
                'project_uuid': bundle_manifest.project_uuid,
                'bundle_fqid': bundle_manifest.fqid,
                'not_found': True
            }

        report = {
            'project_uuid': bundle_manifest.project_uuid,
            'bundle_fqid': bundle_manifest.fqid,
            'has_links_to_correct': False
        }

        links = bundle_manifest.correct_links()
        if links != bundle_manifest.links:
            report['has_links_to_correct'] = True

        return report


def find_projects():
    tracker_url = 'https://tracker-api.data.humancellatlas.org/v0/projects'
    r = requests.get(tracker_url)
    r.raise_for_status()
    result = r.json()
    project_uuids = list(result.keys())
    return project_uuids


def get_summary(map_results):
    summary = {}
    for result in map_results:
        project_uuid = result['project_uuid']
        project_summary = summary.get(project_uuid, {
            'bundles_to_correct': 0,
            'not_found': 0,
            'bundle_count': 0
        })
        project_summary['bundle_count'] = project_summary['bundle_count'] + 1
        if result.get('has_links_to_correct'):
            project_summary['bundles_to_correct'] = project_summary['bundles_to_correct'] + 1
        if result.get('not_found'):
            project_summary['not_found'] = project_summary['not_found'] + 1
        summary[project_uuid] = project_summary

    return {
        'map_results': map_results,
        'summary': summary
    }


def save_json_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f'Saved {filename}')


class BundleManifestResourceProcessor:
    def __init__(self, env, project_uuid):
        self.env = env
        self.project_uuid = project_uuid

        infix = f'.{env}' if env != 'prod' else ''
        dss_url = f'https://dss{infix}.data.humancellatlas.org'
        ingest_url = f'https://api.ingest{infix}.data.humancellatlas.org'

        self.ingest_api = IngestApi(ingest_url)
        self.dss_api = DssApi(dss_url)
        self.bundle_manifest_service = BundleManifestService(ingest_api, dss_api)

    def map_bundle_resource(self, bundle_manifest_resource):
        bundle_manifest = BundleManifest(self.dss_api, self.project_uuid, bundle_manifest_resource)
        return self.bundle_manifest_service.process_bundle_manifest(bundle_manifest)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script to populate the new "links" field in the '
                    'BundleManifest for bundles in a given project uuid')
    parser.add_argument('env',
                        choices=['dev', 'integration', 'staging', 'prod'],
                        help='environment')
    parser.add_argument('project_uuid', metavar='project-uuid', type=str,
                        help='Project uuid')
    args = parser.parse_args()

    # project_uuid = args.project_uuid

    env = args.env

    infix = f'.{env}' if env != 'prod' else ''
    dss_url = f'https://dss{infix}.data.humancellatlas.org'
    ingest_url = f'https://api.ingest{infix}.data.humancellatlas.org'

    ingest_api = IngestApi(ingest_url)
    dss_api = DssApi(dss_url)
    bundle_manifest_service = BundleManifestService(ingest_api, dss_api)

    # As of 29 July
    project_uuids = [
        # "f8aa201c-4ff1-45a4-890e-840d63459ca2",  # done
        # "091cf39b-01bc-42e5-9437-f419a66c8a45",  # done
        # "008e40e8-66ae-43bb-951c-c073a2fa6774",  # done
        # "f86f1ab4-1fbb-4510-ae35-3ffd752d4dfc",  # done
        # "cc95ff89-2e68-4a08-a234-480eca21ce79",  # done
        # "f81efc03-9f56-4354-aabb-6ce819c3d414",  # done
        # "027c51c6-0719-469f-a7f5-640fe57cbece",  # done
        # "74b6d569-3b11-42ef-b6b1-a0454522b4a0",
        # "90bd6933-40c0-48d4-8d76-778c103bf545",
        # "a29952d9-925e-40f4-8a1c-274f118f1f51",  # has no links.json!!!
        # "005d611a-14d5-4fbf-846e-571a1f874f70",  # has analysis done
        # "a9c022b4-c771-4468-b769-cabcf9738de3",  # done
        # "c4077b3c-5c98-4d26-a614-246d12c2e5d7",  # has analysis - done
        # "f83165c5-e2ea-4d15-a5cf-33f3550bffde",  # has analysis - done
        # "2043c65a-1cf8-4828-a656-9e247d4e64f1",  # has analysis - done
        # "8c3c290d-dfff-4553-8868-54ce45f4ba7f",
        # "ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad",
        # "cddab57b-6868-4be4-806f-395ed9dd635a",  # has analysis done
        # "f306435b-4e60-4a79-83a1-159bda5a2c79"   # Tabula Muris - no need
    ]
    start_time = time.time()
    print(f'Found {len(project_uuids)} projects')
    for project_uuid in project_uuids:
        bundle_manifest_resources = bundle_manifest_service.find_bundle_manifests(project_uuid)
        count = bundle_manifest_service.find_bundle_manifests_count(project_uuid)
        print(f'Project {project_uuid} has {count} bundles')
        thread_pool = multiprocessing.Pool(PROCESS_COUNT)
        map_results = thread_pool.map(BundleManifestResourceProcessor(env, project_uuid).map_bundle_resource, bundle_manifest_resources)
        summary = get_summary(map_results)
        save_json_to_file(summary, f'{project_uuid}_summary.log')
    print(f"{PROCESS_COUNT} processes -- Execution Time: {time.time() - start_time} seconds")