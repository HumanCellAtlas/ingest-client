#!/usr/bin/env python
"""
desc goes here
"""
import requests

__author__ = "jupp"
__license__ = "Apache 2.0"


import json
import logging
import os
import uuid
import time
import polling

from urllib.parse import urljoin

import ingest.api.dssapi as dssapi
import ingest.api.ingestapi as ingestapi
import ingest.api.stagingapi as stagingapi
from requests.exceptions import HTTPError

DEFAULT_INGEST_URL = os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL = os.environ.get('STAGING_API', 'http://upload.dev.data.humancellatlas.org')
DEFAULT_DSS_URL = os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')

BUNDLE_SCHEMA_BASE_URL = os.environ.get('BUNDLE_SCHEMA_BASE_URL', 'https://schema.humancellatlas.org')


# TODO shouldn't source from environment variables, must pass config or params instead, throw an error if not in config

class IngestExporter:
    def __init__(self, options=None):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        self.logger = logging.getLogger(__name__)

        self.dryrun = options.dry if options and options.dry else False
        self.outputDir = options.output if options and options.output else None

        self.ingestUrl = options.ingest if options and options.ingest else os.path.expandvars(DEFAULT_INGEST_URL)

        self.stagingUrl = options.staging if options and options.staging else os.path.expandvars(DEFAULT_STAGING_URL)
        self.dssUrl = options.dss if options and options.dss else os.path.expandvars(DEFAULT_DSS_URL)

        self.staging_api = stagingapi.StagingApi()
        self.dss_api = dssapi.DssApi()
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)
        self.related_entities_cache = {}

    def export_bundle(self, submission_uuid, process_uuid):
        start_time = time.time()
        self.related_entities_cache = {}
        saved_bundle_uuid = None

        if not self.dryrun and not self.staging_api.hasStagingArea(submission_uuid):
            error_message = "Can't do export as no upload area has been created."
            raise NoUploadAreaFoundError(error_message)

        self.logger.info('Export bundle for process with UUID ' + process_uuid)

        self.logger.info('Retrieving all process information...')

        process = self.ingest_api.getEntityByUuid('processes', process_uuid)
        process_info = self.get_all_process_info(process)

        self.logger.info('Generating bundle files...')
        submission = self.ingest_api.getEntityByUuid('submissionEnvelopes', submission_uuid)
        is_indexed = submission['triggersAnalysis']

        metadata_by_type = self.get_metadata_by_type(process_info)
        files_by_type = self.prepare_metadata_files(metadata_by_type, process_info, is_indexed)

        links = self.bundle_links(process_info.links)
        links_file_uuid = str(uuid.uuid4())
        files_by_type['links'] = list()
        files_by_type['links'].append({
            'content': links,
            'content_type': '"metadata/{0}"'.format('links'),
            'indexed': is_indexed,
            'dss_filename': 'links.json',
            'dss_uuid': links_file_uuid,
            'upload_filename': 'links_' + links_file_uuid + '.json'
        })

        # restructure bundle manifest
        bundle_manifest = self.create_bundle_manifest(submission_uuid, files_by_type)

        self.logger.info('Generating bundle files...')

        if self.dryrun:
            self.logger.info('Export is using dry run mode.')
            self.logger.info('Dumping bundle files...')

            for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
                for metadata_doc in files_by_type[metadata_type]:
                    bundle_file = metadata_doc
                    filename = bundle_file['upload_filename']
                    content = bundle_file['content']
                    output_dir = self.outputDir if self.outputDir else bundle_manifest.bundleUuid
                    self.dump_to_file(json.dumps(content, indent=4), filename, output_dir=output_dir)

            self.logger.info('Dry run for bundle ' + bundle_manifest.bundleUuid)
            self.logger.info("Execution Time: %s seconds" % (time.time() - start_time))
        else:
            self.logger.info('Uploading metadata files...')
            self.upload_metadata_files(submission_uuid, files_by_type)

            metadata_files = self.get_metadata_files(files_by_type)
            data_files = self.get_data_files(metadata_by_type['file'])
            bundle_files = metadata_files + data_files

            bundle_manifest.dataFiles = list()
            bundle_manifest.dataFiles = [data_file['dss_uuid'] for data_file in data_files]
            self.logger.info('Saving files in DSS...')
            bundle_uuid = bundle_manifest.bundleUuid
            created_files = self.put_files_in_dss(bundle_uuid, bundle_files, process_info)

            # check all created files
            self.logger.info('Verifying if all files get successfully copied to DSS...')
            self.verify_files(created_files)

            self.logger.info('Saving bundle in DSS...')
            self.put_bundle_in_dss(bundle_uuid, created_files)

            self.logger.info('Saving bundle manifest...')
            self.ingest_api.createBundleManifest(bundle_manifest)

            saved_bundle_uuid = bundle_manifest.bundleUuid

            self.logger.info('Bundle ' + bundle_uuid + ' was successfully created!')
            self.logger.info("Execution Time: %s seconds" % (time.time() - start_time))

        return saved_bundle_uuid

    def get_metadata_by_type(self, process_info: 'ProcessInfo') -> dict:
        #  given a ProcessInfo, pull out all the metadata and return as a map of UUID->metadata documents
        simplified = dict()
        simplified['process'] = dict(process_info.derived_by_processes)
        simplified['biomaterial'] = dict(process_info.input_biomaterials)
        simplified['protocol'] = dict(process_info.protocols)
        simplified['file'] = dict(process_info.derived_files)
        simplified['file'].update(process_info.input_files)
        simplified['file'].update(process_info.supplementary_files)

        simplified['project'] = dict()
        simplified['project'][process_info.project['uuid']['uuid']] = process_info.project

        return simplified

    def get_all_process_info(self, process):
        process_info = ProcessInfo()
        process_info.input_bundle = self.get_input_bundle(process)

        process_info.project = self.get_project_info(process)

        if not process_info.project:  # get from input bundle
            project_uuid_lists = list(process_info.input_bundle['fileProjectMap'].values())

            if len(project_uuid_lists) == 0 and len(project_uuid_lists[0]) == 0:
                raise Error('Input bundle manifest has no list of project uuid.')  # very unlikely to happen

            project_uuid = project_uuid_lists[0][0]
            process_info.project = self.ingest_api.getProjectByUuid(project_uuid)

        self.recurse_process(process, process_info)

        if process_info.project:
            supplementary_files = self.ingest_api.getRelatedEntities('supplementaryFiles', process_info.project, 'files')
            for supplementary_file in supplementary_files:
                uuid = supplementary_file['uuid']['uuid']
                process_info.supplementary_files[uuid] = supplementary_file

        return process_info

    def get_project_info(self, process):
        projects = list(self.ingest_api.getRelatedEntities('projects', process, 'projects'))

        if len(projects) > 1:
            raise MultipleProjectsError('Can only be one project in bundle')

        # TODO add checking for project only on an assay process
        # TODO an analysis process may have no link to a project

        if len(projects) > 0:
            return projects[0]

        return None

    # get all related info of a process
    def recurse_process(self, process, process_info):
        uuid = process['uuid']['uuid']
        process_info.derived_by_processes[uuid] = process

        # get all derived by processes using input biomaterial and input files
        derived_by_processes = []

        # wrapper process has the links to input biomaterials and derived files to check if a process is an assay
        input_biomaterials = self.get_related_entities('inputBiomaterials', process, 'biomaterials')
        for input_biomaterial in input_biomaterials:
            uuid = input_biomaterial['uuid']['uuid']
            process_info.input_biomaterials[uuid] = input_biomaterial
            derived_by_processes.extend(
                self.get_related_entities('derivedByProcesses', input_biomaterial, 'processes'))

        input_files = self.get_related_entities('inputFiles', process, 'files')
        for input_file in input_files:
            uuid = input_file['uuid']['uuid']
            process_info.input_files[uuid] = input_file
            derived_by_processes.extend(
                self.get_related_entities('derivedByProcesses', input_file, 'processes'))

        derived_biomaterials = self.get_related_entities('derivedBiomaterials', process, 'biomaterials')
        derived_files = self.get_related_entities('derivedFiles', process, 'files')

        process_uuid = process['uuid']['uuid']

        protocols = self.get_related_entities('protocols', process, 'protocols')
        for protocol in protocols:
            uuid = protocol['uuid']['uuid']
            process_info.protocols[uuid] = protocol

        for derived_file in derived_files:
            uuid = derived_file['uuid']['uuid']
            process_info.derived_files[uuid] = derived_file

        if input_biomaterials:
            if derived_files:
                process_info.links.append({
                    'process': process_uuid,
                    'inputs': [input_biomaterial['uuid']['uuid'] for input_biomaterial in input_biomaterials],
                    'input_type': 'biomaterial',
                    'outputs': [derived_file['uuid']['uuid'] for derived_file in derived_files],
                    'output_type': 'file',
                    'protocols': [
                        {
                            'protocol_type': self.get_concrete_entity_type(protocol),
                            'protocol_id': protocol['uuid']['uuid']
                        } for protocol in protocols
                    ]
                })

            if derived_biomaterials:
                process_info.links.append({
                    'process': process_uuid,
                    'inputs': [input_biomaterial['uuid']['uuid'] for input_biomaterial in input_biomaterials],
                    'input_type': 'biomaterial',
                    'outputs': [derived_biomaterial['uuid']['uuid'] for derived_biomaterial in derived_biomaterials],
                    'output_type': 'biomaterial',
                    'protocols': [
                        {
                            'protocol_type': self.get_concrete_entity_type(protocol),
                            'protocol_id': protocol['uuid']['uuid']
                        } for protocol in protocols
                    ]
                })

        if input_files and derived_files:
            process_info.links.append({
                'process': process_uuid,
                'inputs': [input_file['uuid']['uuid'] for input_file in input_files],
                'input_type': 'file',
                'outputs': [derived_file['uuid']['uuid'] for derived_file in derived_files],
                'output_type': 'file',
                'protocols': [
                    {
                        'protocol_type': self.get_concrete_entity_type(protocol),
                        'protocol_id': protocol['uuid']['uuid']
                    } for protocol in protocols
                ]
            })

        for derived_by_process in derived_by_processes:
            self.recurse_process(derived_by_process, process_info)

    def get_related_entities(self, relationship, entity, entity_type):
        entity_uuid = entity['uuid']['uuid']

        if self.related_entities_cache.get(entity_uuid) and self.related_entities_cache.get(entity_uuid).get(relationship):
            return self.related_entities_cache.get(entity_uuid).get(relationship)

        related_entities = list(self.ingest_api.getRelatedEntities(relationship, entity, entity_type))

        if not self.related_entities_cache.get(entity_uuid):
            self.related_entities_cache[entity_uuid] = {}

        if not self.related_entities_cache.get(entity_uuid).get(relationship):
            self.related_entities_cache[entity_uuid][relationship] = []

        self.related_entities_cache[entity_uuid][relationship] = related_entities

        return related_entities

    def get_input_bundle(self, process):
        bundle_manifests = list(self.ingest_api.getRelatedEntities('inputBundleManifests', process, 'bundleManifests'))

        if len(bundle_manifests) > 0:
            return bundle_manifests[0]

        return None

    def prepare_metadata_files(self, metadata_info, process_info, is_indexed=True) -> 'dict':
        metadata_files_by_type = dict()

        for entity_type in ['biomaterial', 'file', 'project', 'protocol', 'process']:
            metadata_files_by_type[entity_type] = list()
            concrete_type_ctr = dict()
            for (metadata_uuid, doc) in metadata_info[entity_type].items():
                concrete_type = self.get_concrete_entity_type(doc)

                concrete_type_ctr[concrete_type] = 0 if concrete_type not in concrete_type_ctr else concrete_type_ctr[concrete_type] + 1

                file_name = '{0}_{1}.json'.format(concrete_type, concrete_type_ctr[concrete_type])
                upload_filename = '{0}_{1}.json'.format(concrete_type, metadata_uuid)

                prepared_doc = {
                    'content': self.bundle_metadata(doc, metadata_uuid),
                    'content_type': '"metadata/{0}"'.format(entity_type),
                    'indexed': is_indexed,
                    'dss_filename': file_name,
                    'dss_uuid': metadata_uuid,
                    'upload_filename': upload_filename,
                    'update_date': doc['updateDate'],
                    'is_from_input_bundle': self._is_from_input_bundle(entity_type, metadata_uuid, process_info.input_bundle)
                }

                metadata_files_by_type[entity_type].append(prepared_doc)

        return metadata_files_by_type

    def _is_from_input_bundle(self, entity_type, metadata_uuid, input_bundle):

        field = {
            'biomaterial': 'fileBiomaterialMap',
            'process': 'fileProcessMap',
            'file': 'fileFilesMap',
            'project': 'fileProjectMap',
            'protocol': 'fileProtocolMap',
        }

        return input_bundle and input_bundle[field[entity_type]].get(metadata_uuid)

    def bundle_metadata(self, metadata_doc, uuid):
        provenance_core = dict()
        provenance_core['document_id'] = uuid
        provenance_core['submission_date'] = metadata_doc['submissionDate']
        provenance_core['update_date'] = metadata_doc['updateDate']

        bundle_doc = metadata_doc['content']
        bundle_doc['provenance'] = provenance_core

        return bundle_doc

    def bundle_links(self, links):

        latest_schema = self.ingest_api.get_schemas(
            latest_only=True,
            high_level_entity='system',
            domain_entity='',
            concrete_entity='links'
        )

        schema_url = latest_schema[0]['_links']['json-schema']['href'] if latest_schema else None
        schema_version = latest_schema[0]['schemaVersion'] if latest_schema else None

        return {
            'describedBy': schema_url,
            'schema_type': 'link_bundle',
            'schema_version': schema_version,
            'links': links
        }

    def upload_metadata_files(self, submission_uuid, metadata_files_info):
        try:
            for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
                for metadata_doc in metadata_files_info[metadata_type]:
                    bundle_file = metadata_doc
                    filename = bundle_file['upload_filename']
                    content = bundle_file['content']
                    content_type = bundle_file['content_type']

                    if not bundle_file.get('is_from_input_bundle'):
                        uploaded_file = self.upload_file(submission_uuid, filename, content, content_type)
                        bundle_file['upload_file_url'] = uploaded_file.url
        except Exception as e:
            message = "An error occurred on uploading bundle files: " + str(e)
            raise BundleFileUploadError(message)

    def put_bundle_in_dss(self, bundle_uuid, created_files):
        try:
            created_bundle = self.dss_api.put_bundle(bundle_uuid, created_files)
        except Exception as e:
            message = 'An error occurred while putting bundle in DSS: ' + str(e)
            raise BundleDSSError(message)

        return created_bundle

    def put_files_in_dss(self, bundle_uuid, files_to_put, process_info):
        created_files = []

        for bundle_file in files_to_put:
            version = ''
            file_uuid = bundle_file["dss_uuid"]
            created_file = None
            input_data_files = [input_file['dataFileUuid'] for input_file in list(process_info.input_files.values())]

            try:
                # TODO if file is an input file, this file may already be in the data store, need to get the stored version
                # This assumes that the latest version is the file version in the input bundle, should be a safe assumption for now
                # Ideally, bundle manifest must store the file uuid and version and version must be retrieved from there

                # if metadata file , check is_from_input_bundle flag, if true, do not put file to DSS again
                if bundle_file.get('is_from_input_bundle') or file_uuid in input_data_files:
                    file_response = self.dss_api.head_file(bundle_file["dss_uuid"])
                    created_file = {
                        'version': file_response.headers['X-DSS-VERSION']
                    }
                else:
                    created_file = self.dss_api.put_file(bundle_uuid, bundle_file)

                version = created_file['version']
            except Exception as e:
                raise FileDSSError('An error occurred while putting file in DSS' + str(e))

            file_param = {
                "indexed": bundle_file["indexed"],
                "name": bundle_file["submittedName"],
                "uuid": file_uuid,
                "content-type": bundle_file["content-type"],
                "version": version
            }

            created_files.append(file_param)

        return created_files

    def verify_files(self, created_files):
        for created_file in created_files:
            try:
                polling.poll(
                    lambda: self._is_file_copied(created_file),
                    step=30,
                    timeout=1200  # 20 minutes
                )
            except polling.TimeoutException as te:
                self.logger.error(f'File {created_file["uuid"]}/{created_file["version"]} with name {created_file["name"]} takes too long to be copied.')
                raise
            self.logger.info(f'File {created_file["uuid"]}/{created_file["version"]} with name {created_file["name"]} is successfully copied!')

    def _is_file_copied(self, created_file):
        try:
            r = self.dss_api.head_file(created_file["uuid"], version=created_file["version"])
            return (r.status_code == requests.codes.ok) or (r.status_code == requests.codes.created)
        except Exception as e:
            return False

    def get_metadata_files(self, metadata_files_info):
        metadata_files = []

        for entity_type in ['biomaterial', 'file', 'project', 'protocol', 'process', 'links']:
            for metadata_file in metadata_files_info[entity_type]:
                metadata_files.append({
                    'name': metadata_file['upload_filename'],
                    'submittedName': metadata_file['dss_filename'],
                    'url': metadata_file.get('upload_file_url'),
                    'dss_uuid': metadata_file['dss_uuid'],
                    'indexed': metadata_file['indexed'],
                    'content-type': metadata_file['content_type'],
                    'update_date': metadata_file.get('update_date'),
                    'is_from_input_bundle': metadata_file.get('is_from_input_bundle')
                })
        return metadata_files

    def get_data_files(self, uuid_file_dict):
        data_files = []
        #  TODO: need to keep track of UUIDs used so that retries work when the DSS returns a 500
        for file_uuid, data_file in uuid_file_dict.items():
            filename = data_file['fileName']
            cloud_url = data_file['cloudUrl']
            data_file_uuid = data_file['dataFileUuid']

            data_files.append({
                'name': filename,
                'submittedName': filename,
                'url': cloud_url,
                'dss_uuid': data_file_uuid,
                'indexed': False,
                'content-type': 'data'
            })

        return data_files

    def create_bundle_manifest(self, submission_uuid, files_by_type):
        bundle_manifest = ingestapi.BundleManifest()
        bundle_manifest.envelopeUuid = submission_uuid

        bundle_manifest.fileProjectMap = dict()
        for metadata_file in files_by_type['project']:
            bundle_manifest.fileProjectMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileBiomaterialMap = dict()
        for metadata_file in files_by_type['biomaterial']:
            bundle_manifest.fileBiomaterialMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileProcessMap = dict()
        for metadata_file in files_by_type['process']:
            bundle_manifest.fileProcessMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileProtocolMap = dict()
        for metadata_file in files_by_type['protocol']:
            bundle_manifest.fileProtocolMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileFilesMap = dict()
        for metadata_file in files_by_type['file']:
            bundle_manifest.fileFilesMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        return bundle_manifest

    def get_concrete_entity_type(self, schema_uri):
        return schema_uri["content"]["describedBy"].rsplit('/', 1)[-1]

    def upload_file(self, submission_uuid, filename, content, content_type):
        file_description = self.staging_api.getFile(submission_uuid, filename)

        if file_description:
            self.logger.info(f"The file {filename} already exists in the Upload area {submission_uuid}.")
        else:
            self.logger.info("Writing to staging area..." + filename)
            try:
                file_description = self.staging_api.stageFile(submission_uuid, filename, content, content_type)
            except HTTPError as e:
                if str(e.response.status_code) == "409":
                    file_description = self.staging_api.getFile(submission_uuid, filename)
                    if file_description:
                        return file_description
                    else:
                        raise e
                raise e

        self.logger.info("File staged at " + file_description.url)
        return file_description

    def dump_to_file(self, content, filename, output_dir='output'):
        directory = os.path.abspath(output_dir)

        if not os.path.exists(directory):
            os.makedirs(directory)

        tmp_file = open(directory + "/" + filename + ".json", "w")
        tmp_file.write(content)
        tmp_file.close()

class File:
    def __init__(self):
        self.name = ""
        self.content_type = ""
        self.size = ""
        self.id = ""
        self.checksums = {}


class ProcessInfo:
    def __init__(self):
        self.project = {}

        # uuid => object mapping
        self.input_biomaterials = {}
        self.derived_by_processes = {}
        self.input_files = {}
        self.derived_files = {}
        self.protocols = {}
        self.supplementary_files = {}

        self.links = []

        self.input_bundle = None


# Module Exceptions


class Error(Exception):
    """Base-class for all exceptions raised by this module."""


class MultipleProjectsError(Error):
    """A process should only have one project linked."""


class InvalidBundleError(Error):
    """There was a failure in bundle validation."""

class BundleFileUploadError(Error):
    """There was a failure in bundle file upload."""


class BundleDSSError(Error):
    """There was a failure in bundle creation in DSS."""


class FileDSSError(Error):
    """There was a failure in file creation in DSS."""


class NoUploadAreaFoundError(Error):
    """Export couldn't be as no upload area found"""