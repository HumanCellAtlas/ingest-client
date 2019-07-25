import argparse

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi


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

    project_uuid = args.project_uuid
    env = args.env

    infix = f'.{env}' if env != 'prod' else ''
    dss_url = f'https://dss{infix}.data.humancellatlas.org'
    ingest_url = f'https://api.ingest{infix}.data.humancellatlas.org'

    ingest_api = IngestApi(ingest_url)
    dss_api = DssApi(dss_url)

    project = ingest_api.get_project_by_uuid(project_uuid)
    bundle_manifests = ingest_api.get_related_entities("bundleManifests",
                                                       project,
                                                       "bundleManifests")

    for bundle_manifest in bundle_manifests:
        bundle_uuid = bundle_manifest.get('bundleUuid')
        version = bundle_manifest.get('bundleVersion')

        bundle_json = dss_api.get_bundle(bundle_uuid, version)
        bundle = bundle_json.get('bundle')
        files_map = {file['name']: file for file in bundle.get('files')}

        links_file = files_map['links.json']
        links_file_uuid = links_file['uuid']
        links_file_version = links_file['version']
        links_file_json = dss_api.get_file(links_file_uuid, links_file_version)
        links = links_file_json.get('links')

        bundle_manifest_url = bundle_manifest['_links']['self']['href']
        ingest_api.patch(bundle_manifest_url, {'links': links})
