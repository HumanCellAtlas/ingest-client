#!/usr/bin/env python
import json
import logging
import os
import re
import time
import uuid
from copy import deepcopy
from typing import Dict, Set, Any, List
from dataclasses import dataclass

import polling
import requests

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi
from ingest.exporter.metadata import MetadataResource, DataFile
from ingest.exporter.storage import StorageService
from ingest.exporter.experiment_exporter.graph_crawler import GraphCrawler, ExperimentGraph, LinkSet


DEFAULT_INGEST_URL = os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL = os.environ.get('STAGING_API', 'http://upload.dev.data.humancellatlas.org')
DEFAULT_DSS_URL = os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')



class BundleableEntity:



class ExperimentExporter:
    def __init__(self, ingest_api: IngestApi, storage_service: StorageService, dry_run=False,
                 output_directory=None):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        self.logger = logging.getLogger(__name__)

        self.dry_run = dry_run
        self.output_dir = output_directory

        self.storage_service = storage_service
        self.ingest_api = ingest_api
        self.graph_crawler = GraphCrawler(self.ingest_api)
        self.related_entities_cache = {}

    def export_bundle(self, bundle_uuid, bundle_version, submission_uuid, process_uuid):
        process = self.get_process(process_uuid)
        submission = self.get_submission(submission_uuid)
        staging_area_uuid = ExperimentExporter.get_staging_area_uuid(submission)

        is_indexed = submission['triggersAnalysis']
        project = self.project_for_submission(submission)

        # generate the experiment graph, add the Project as a dangling node
        experiment_graph = self.graph_crawler.experiment_graph_for_process(process)
        experiment_graph.nodes.add_node(project)

        # store the experiment files, data files, and the links.json
        data_files = ExperimentExporter.data_files_for_experiment(experiment_graph)
        links_json = ExperimentExporter.links_json(experiment_graph.links, str(uuid.uuid4()))
        links_json = MetadataResource.from_dict(links_json)
        stored_files = [self.storage_service.store_metadata(m, staging_area_uuid)
                        for m in experiment_graph.nodes.get_nodes()]
        stored_files.extend([self.storage_service.store_data_file(data_file) for data_file in data_files])
        stored_files.append(self.storage_service.store_metadata(links_json, staging_area_uuid))




    @staticmethod
    def data_files_for_experiment(experiment_graph: ExperimentGraph) -> List[DataFile]:
        return [DataFile.from_file_metadata(m) for m in experiment_graph.nodes.get_nodes() if m.metadata_type == "file"]

    def get_process(self, process_uuid) -> MetadataResource:
        return self.ingest_api.get_entity_by_uuid('processes', process_uuid)


    def get_submission(self, submission_uuid):
        return self.ingest_api.get_entity_by_uuid('submissionEnvelopes', submission_uuid)

    @staticmethod
    def get_staging_area_uuid(submission: dict) -> str:
        return submission["uuid"]["uuid"]

    def project_for_submission(self, submission) -> MetadataResource:
        pass

    @staticmethod
    def links_json(link_set: LinkSet, links_json_uuid: str) -> dict:
        return {
            'uuid': {
                'uuid': links_json_uuid
            },
            'type': 'links',
            'content': [l.to_dict() for l in link_set.get_links()],
            'dcpVersion': None
        }





