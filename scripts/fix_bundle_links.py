import argparse
import json

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi
from ingest.exporter.ingestexportservice import LinkSet


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

        for resource in bundle_manifests:
            bundle_manifest = BundleManifest(self.dss_api, resource)
            bundle_uuid = bundle_manifest.uuid
            version = bundle_manifest.version

            bundle = Bundle(dss_api, bundle_uuid, version)
            self.ingest_api.patch(bundle_manifest.url, {'links': bundle.links})

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
            bundle_manifest = BundleManifest(self.dss_api, resource)
            print(f'Processing {i + 1}/{count} , bundle {bundle_manifest.fqid} ...')
            report = bundle_manifest.remove_erroneous_links()
            if report:
                reports[bundle_manifest.fqid] = report

        reports['summary']['error_count'] = len(list(reports['bundles'].keys()))
        return reports


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
    def __init__(self, dss_api, resource):
        self._object = resource
        self.dss_api = dss_api

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
            bundle = Bundle(self.dss_api, self.uuid, self.version)
            return bundle.links
        return self._object.get('links')

    def remove_duplicate_links(self, links):
        link_set = LinkSet()
        for link in links:
            link_set.add_link(link)
        new_links = link_set.get_links()

        if len(links) != len(new_links):
            return new_links

        return links

    def remove_supplementary_file_links(self, links):
        new_links = []
        for link in links:
            if not self._is_supplementary_file_link(link):
                new_links.append(link)
        if len(new_links) != len(self.links):
            return new_links

        return self.links

    def remove_erroneous_links(self):
        report = {}
        new_links = self.remove_duplicate_links(self.links)
        newer_links = self.remove_supplementary_file_links(new_links)

        if len(new_links) != len(self.links):
            report['has_duplicates'] = True

        if len(newer_links) != len(new_links):
            report['has_supplementary_files'] = True

        if len(newer_links) == len(new_links) and len(self.links) == len(new_links):
            return None

        report['newer_links'] = newer_links
        report['old_links'] = self.links
        return report

    def _is_supplementary_file_link(self, link):
        input_type = link.get('input_type')
        if input_type == 'file':
            inputs = link.get('inputs')
            for file_uuid in inputs:
                file_json = self.dss_api.get_file(file_uuid)
                described_by = file_json.get('describedBy')
                schema_type = described_by.rsplit('/', 1)[-1]
                if schema_type == 'supplementary_file':
                    return True

        return False


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

    bundle_manifest_service = BundleManifestService(ingest_api, dss_api)
    # bundle_manifest_service.update_bundle_manifest_links(project_uuid)
    report = bundle_manifest_service.find_bundles_with_links_correction(project_uuid)
    report_filename = f'project_{project_uuid}_bundles_report.json'
    with open(report_filename, 'w') as report_file:
        json.dump(report, report_file, indent=4)

    print(f'Saved {report_filename}')