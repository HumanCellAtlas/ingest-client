#!/usr/bin/env python3

from ingest.api.ingestapi import IngestApi
from dataclasses import dataclass
import json
import datetime
import argparse
import uuid


class EntityMap:
    def __init__(self):
        self.uuid_map = dict()
        self.uri_map = dict()

    def add(self, entities: list, uuid_path: list = None):
        for e in entities:
            uri = e["_links"]["self"]["href"]
            self.uri_map[uri] = e
            if uuid_path:
                uuid = EntityMap.value_at_path(uuid_path, e)
            else:
                uuid = e["uuid"]["uuid"]
            self.uuid_map[uuid] = e

    def uuids(self):
        return list(self.uuid_map.keys())

    def uris(self):
        return list(self.uri_map.keys())

    def ids(self, with_uris=False):
        return self.uris() if with_uris else self.uuids()

    @staticmethod
    def from_list(entities: list, uuid_path: list = None):
        e = EntityMap()
        e.add(entities, uuid_path)
        return e

    @staticmethod
    def value_at_path(path, data):
        return EntityMap.value_at_path(path[1:], data[path[0]]) if len(path) > 0 else data

    def __len__(self):
        return len(self.uuid_map)

    def __iter__(self):
        return list(self.uuid_map.values()).__iter__()


@dataclass
class EntityRedaction:
    sequence_files: EntityMap = EntityMap()
    supplementary_files: EntityMap = EntityMap()
    biomaterials: EntityMap = EntityMap()
    processes: EntityMap = EntityMap()
    protocols: EntityMap = EntityMap()
    projects: EntityMap = EntityMap()
    primary_bundle_manifests: EntityMap = EntityMap()
    primary_submissions: EntityMap = EntityMap()

    analysis_processes: EntityMap = EntityMap()
    analysis_protocols: EntityMap = EntityMap()
    analysis_files: EntityMap = EntityMap()
    analysis_bundle_manifests: EntityMap = EntityMap()
    analysis_submissions: EntityMap = EntityMap()

    def to_redaction_manifest(self, with_uris=False) -> dict:
        return {
            "sequence_files": self.sequence_files.ids(with_uris),
            "supplementary_files": self.supplementary_files.ids(with_uris),
            "biomaterials": self.biomaterials.ids(with_uris),
            "processes": self.processes.ids(with_uris),
            "protocols": self.protocols.ids(with_uris),
            "projects": self.projects.ids(with_uris),
            "primary_bundle_manifests": self.primary_bundle_manifests.ids(with_uris),
            "primary_submissions": self.primary_submissions.ids(with_uris),

            "analysis_processes": self.analysis_processes.ids(with_uris),
            "analysis_protocols": self.analysis_protocols.ids(with_uris),
            "analysis_files": self.analysis_files.ids(with_uris),
            "analysis_bundle_manifests": self.analysis_bundle_manifests.ids(with_uris),
            "analysis_submissions": self.analysis_submissions.ids(with_uris)
        }


class RedactionManifestUtils:

    @staticmethod
    def create_redaction_manifest(project_uuid: str, ingest_url: str, with_uris=False):
        entity_redaction = RedactionManifestUtils.generate_entity_redaction(project_uuid, ingest_url)
        redaction_manifest = entity_redaction.to_redaction_manifest(with_uris)

        redaction_manifest_filename = f'redact_{project_uuid}_{datetime.datetime.utcnow().isoformat()}.json'
        with open(redaction_manifest_filename, "w") as f:
            print(f'Writing redaction manifest to {redaction_manifest_filename}\n')
            json.dump(redaction_manifest, f)

        print("Finished writing redaction manifest to {redaction_manifest_filename}\n")

    @staticmethod
    def generate_entity_redaction(project_uuid: str, ingest_url: str) -> 'EntityRedaction':
        ingest_client = IngestApi(ingest_url)

        project = ingest_client.get_project_by_uuid(project_uuid)
        submission_for_project = list(ingest_client.get_related_entities("submissionEnvelopes", project, "submissionEnvelopes"))[0]
        submissions_for_project = EntityMap.from_list([submission_for_project])

        files = list(ingest_client.get_related_entities("files", submission_for_project, "files"))
        supplementary_files = EntityMap.from_list([f for f in files if "supplementary_file" in f["content"]["describedBy"]])
        sequence_files = EntityMap.from_list([f for f in files if "sequence_file" in f["content"]["describedBy"]])

        assert len(supplementary_files) + len(sequence_files) == len(files)

        biomaterials = EntityMap.from_list(ingest_client.get_related_entities("biomaterials", submission_for_project, "biomaterials"))
        processes = EntityMap.from_list(ingest_client.get_related_entities("processes", submission_for_project, "processes"))
        protocols = EntityMap.from_list(ingest_client.get_related_entities("protocols", submission_for_project, "protocols"))
        projects = EntityMap.from_list(ingest_client.get_related_entities("projects", submission_for_project, "projects"))
        primary_bundle_manifests = EntityMap.from_list(ingest_client.get_related_entities("bundleManifests", submission_for_project, "bundleManifests"), ["bundleUuid"])

        analysis_processes = EntityMap()
        analysis_protocols = EntityMap()
        analysis_files = EntityMap()
        analysis_submissions = EntityMap()
        analysis_bundle_manifests = EntityMap()

        for file in sequence_files:
            input_to_processes = ingest_client.get_related_entities("inputToProcesses", file, "processes")
            for analysis_process in input_to_processes:
                analysis_processes.add([analysis_process])
                analysis_protocols.add(ingest_client.get_related_entities("protocols", analysis_process, "protocols"))
                analysis_files.add(ingest_client.get_related_entities("derivedFiles", analysis_process, "files"))
                analysis_submission = list(ingest_client.get_related_entities("submissionEnvelopes", analysis_process, "submissionEnvelopes"))[0]
                analysis_submissions.add([analysis_submission])
                analysis_bundle_manifests.add(ingest_client.get_related_entities("bundleManifests", analysis_submission, "bundleManifests"), ["bundleUuid"])

        assert len(projects.uuids()) == 1

        return EntityRedaction(sequence_files, supplementary_files, biomaterials, processes, protocols, projects,
                               primary_bundle_manifests, submissions_for_project, analysis_processes,
                               analysis_protocols, analysis_files, analysis_bundle_manifests, analysis_submissions)

    @staticmethod
    def envs():
        return {
            "dev": "https://api.ingest.dev.data.humancellatlas.org",
            "integration": "https://api.ingest.integration.data.humancellatlas.org",
            "staging": "https://api.ingest.staging.data.humancellatlas.org",
            "prod": "https://api.ingest.data.humancellatlas.org"
        }

    @staticmethod
    def ingest_api_url(env: str):
        return RedactionManifestUtils.envs()[env]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a redaction manifest for the HCA DCP ingestion service.\n "
                                                 "Given a project UUID, the redaction manifest will specify the "
                                                 "entities in the project to be redacted.")

    parser.add_argument('project-uuid', type=str, help='UUID of project to redact')
    parser.add_argument('--env', type=str, help='Environment to target')
    parser.add_argument('--ingest-url', type=int, help='URL of the ingest API')

    args = parser.parse_args()

    project_uuid = args.project_uuid
    env = args.env
    ingest_url = args.ingest_url

    try:
        uuid.UUID(project_uuid)
    except Exception:
        parser.error(f'{project_uuid} is not a valid UUID')

    if not env and not ingest_url:
        parser.error("Either --env or --ingest-url must be provided")

    if env not in RedactionManifestUtils.envs():
        parser.error(f'provided env "{env}" not in {list(RedactionManifestUtils.envs().keys())}')

    if not ingest_url:
        ingest_url = RedactionManifestUtils.ingest_api_url(env)

    RedactionManifestUtils.create_redaction_manifest(project_uuid, ingest_url, with_uris=True)
    print("Done\n")
