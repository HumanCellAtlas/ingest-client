#!/usr/bin/env python
"""
desc goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"


import json
import logging
import os

import sys
import uuid
from optparse import OptionParser

import ingest.api.stagingapi as stagingapi
import ingest.api.ingestapi as ingestapi
import ingest.api.dssapi as dssapi

import ingest.utils.bundlevalidator as bundlevalidator

DEFAULT_INGEST_URL = os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL = os.environ.get('STAGING_API', 'http://staging.dev.data.humancellatlas.org')
DEFAULT_DSS_URL = os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')

BUNDLE_SCHEMA_BASE_URL = os.environ.get('BUNDLE_SCHEMA_BASE_URL', 'https://schema.humancellatlas.org/bundle/%s/')
METADATA_SCHEMA_VERSION = os.environ.get('SCHEMA_VERSION', '5.1.0')


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
        self.schema_version = options.schema_version if options and options.schema_version else os.path.expandvars(METADATA_SCHEMA_VERSION)
        self.schema_url = os.path.expandvars(BUNDLE_SCHEMA_BASE_URL % self.schema_version)

        self.logger.debug("ingest url is " + self.ingestUrl)

        self.staging_api = stagingapi.StagingApi()
        self.dss_api = dssapi.DssApi()
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)

    def get_concrete_entity_type(self, schema_uri):
        return schema_uri["content"]["describedBy"].rsplit('/', 1)[-1]

    def build_link_obj(self, source_type, source_id, destination_type, destination_id):
        return {
            'source_type': source_type,
            'source_id': source_id,
            'destination_type': destination_type,
            'destination_id': destination_id
        }

    def generateBundle(self, message):
        success = False
        callbackLink = message["callbackLink"]

        self.logger.info('process received ' + callbackLink)
        self.logger.info('process index: ' + str(message["index"]) + ', total processes: ' + str(message["total"]))

        # given an assay, generate a bundle

        processUrl = self.ingest_api.getAssayUrl(callbackLink)  # TODO rename getAssayUrl
        processUuid = message["documentUuid"]
        envelopeUuid = message["envelopeUuid"]

        # check staging area is available
        if self.dryrun or self.staging_api.hasStagingArea(envelopeUuid):
            assay = self.ingest_api.getAssay(processUrl)

            self.logger.info("Attempting to export bundle to DSS...")
            success = self.export_bundle(envelopeUuid, processUrl)
        else:
            error_message = "Can't do export as no upload area has been created"
            raise NoUploadAreaFoundError(error_message)

        if not success:
            raise Error("An error occurred in export. Failed to export to dss: " + message["callbackLink"])


    def upload_file(self, submission_uuid, filename, content, content_type):
        self.logger.info("writing to staging area..." + filename)
        file_description = self.staging_api.stageFile(submission_uuid, filename, content, content_type)
        self.logger.info("File staged at " + file_description.url)
        return file_description

    def bundle_metadata(self, metadata_doc, uuid):
        provenance_core = dict()
        provenance_core['document_id'] = uuid
        provenance_core['submission_date'] = metadata_doc['submissionDate']
        provenance_core['update_date'] = metadata_doc['updateDate']
        provenance_core['describedBy'] = 'https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/0761cb040dc5b1bb5dd91d38eb7bdfbc2c75e7ea/json_schema/core/provenance_core.json'

        bundle_doc = metadata_doc['content']
        bundle_doc['provenance_core'] = provenance_core

        return bundle_doc

    def dump_to_file(self, content, filename, output_dir=None):
        if output_dir:
            self.outputDir = output_dir

        if self.outputDir:
            dir = os.path.abspath(self.outputDir)
            if not os.path.exists(dir):
                os.makedirs(dir)
            tmpFile = open(dir + "/" + filename + ".json", "w")
            tmpFile.write(content)
            tmpFile.close()

    def export_bundle(self, submission_uuid, process_url):
        self.logger.info('Export bundle for process: ' + process_url)

        saved_bundle_uuid = None

        self.logger.info('Retrieving all process information...')
        process_info = self.get_all_process_info(process_url)
        metadata_by_type = self.get_metadata_by_type(process_info)

        files_by_type = self.prepare_metadata_files(metadata_by_type)

        links = self.bundle_links(process_info.links)
        links_file_uuid = str(uuid.uuid4())
        files_by_type['links'] = list()
        files_by_type['links'].append({
            'content': links,
            'content_type': '"metadata/{0}"'.format('links'),
            'indexed': True,
            'dss_filename': 'links.json',
            'dss_uuid': links_file_uuid,
            'upload_filename': 'links_' + links_file_uuid + '.json'
        })

        # TODO: only store the submission and bundle uuid in bundle manifest for now
        bundle_manifest = self.create_bundle_manifest(submission_uuid)

        self.logger.info('Generating bundle files...')

        if self.dryrun:
            self.logger.info('Export is using dry run mode.')
            self.logger.info('Dumping bundle files...')

            for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
                for metadata_doc in files_by_type[metadata_type]:
                    bundle_file = metadata_doc
                    filename = bundle_file['upload_filename']
                    content = bundle_file['content']
                    self.dump_to_file(json.dumps(content, indent=4), filename, output_dir=bundle_manifest.bundleUuid)

        else:
            self.logger.info('Uploading metadata files...')
            self.upload_metadata_files(submission_uuid, files_by_type)

            self.logger.info('Saving files in DSS...')
            bundle_uuid = bundle_manifest.bundleUuid

            metadata_files = self.get_metadata_files(files_by_type)
            data_files = self.get_data_files(metadata_by_type['file'])
            bundle_files = metadata_files + data_files

            created_files = self.put_files_in_dss(bundle_uuid, bundle_files)

            self.logger.info('Saving bundle in DSS...')
            self.put_bundle_in_dss(bundle_uuid, created_files)

            self.logger.info('Saving bundle manifest...')
            self.ingest_api.createBundleManifest(bundle_manifest)

            saved_bundle_uuid = bundle_manifest.bundleUuid

            self.logger.info('Bundle ' + bundle_uuid + ' was successfully created!')

        return saved_bundle_uuid

    def get_metadata_by_type(self, process_info: 'ProcessInfo') -> dict:
        #  given a ProcessInfo, pull out all the metadata and return as a map of UUID->metadata documents
        simplified = dict()
        simplified['process'] = dict(process_info.derived_by_processes)
        simplified['biomaterial'] = dict(process_info.input_biomaterials)
        simplified['protocol'] = dict(process_info.protocols)
        simplified['file'] = dict(process_info.derived_files)
        simplified['file'].update(process_info.input_files)

        simplified['project'] = dict()
        simplified['project'][process_info.project['uuid']['uuid']] = process_info.project

        return simplified

    def get_all_process_info(self, process_url):
        process = self.ingest_api.getAssay(process_url)  # TODO rename getAssay to getProcess
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
        chained_processes = list(self.ingest_api.getRelatedEntities('chainedProcesses', process, 'processes'))

        is_wrapper = len(chained_processes) > 0

        # don't include wrapper processes in process bundle
        if is_wrapper:
            for chained_process in chained_processes:
                uuid = chained_process['uuid']['uuid']
                process_info.derived_by_processes[uuid] = chained_process
        else:
            uuid = process['uuid']['uuid']
            process_info.derived_by_processes[uuid] = process

        # get all derived by processes using input biomaterial and input files
        derived_by_processes = []

        # wrapper process has the links to input biomaterials and derived files to check if a process is an assay
        input_biomaterials = list(self.ingest_api.getRelatedEntities('inputBiomaterials', process, 'biomaterials'))
        for input_biomaterial in input_biomaterials:
            uuid = input_biomaterial['uuid']['uuid']
            process_info.input_biomaterials[uuid] = input_biomaterial
            derived_by_processes.extend(
                self.ingest_api.getRelatedEntities('derivedByProcesses', input_biomaterial, 'processes'))

        input_files = list(self.ingest_api.getRelatedEntities('inputFiles', process, 'files'))
        for input_file in input_files:
            uuid = input_file['uuid']['uuid']
            process_info.input_files[uuid] = input_file
            derived_by_processes.extend(
                self.ingest_api.getRelatedEntities('derivedByProcesses', input_file, 'processes'))

        derived_biomaterials = list(self.ingest_api.getRelatedEntities('derivedBiomaterials', process, 'biomaterials'))
        derived_files = list(self.ingest_api.getRelatedEntities('derivedFiles', process, 'files'))

        # since wrapper processes are not included in process bundle,
        #  links to it must be applied to its chained processes
        processes_to_link = chained_processes if is_wrapper else [process]
        for process_to_link in processes_to_link:
            process_name = self.get_concrete_entity_type(process_to_link)
            process_uuid = process_to_link['uuid']['uuid']

            for input_biomaterial in input_biomaterials:
                uuid = input_biomaterial['uuid']['uuid']
                process_info.links.append(self.build_link_obj('biomaterial', uuid, process_name, process_uuid))

            for input_file in input_files:
                uuid = input_file['uuid']['uuid']
                process_info.links.append(self.build_link_obj('file', uuid, process_name, process_uuid))

            protocols = list(self.ingest_api.getRelatedEntities('protocols', process_to_link, 'protocols'))
            for protocol in protocols:
                uuid = protocol['uuid']['uuid']
                process_info.links.append(self.build_link_obj(process_name, process_uuid, 'protocol', uuid))
                process_info.protocols[uuid] = protocol

            for derived_file in derived_files:
                uuid = derived_file['uuid']['uuid']
                process_info.links.append(self.build_link_obj(process_name, process_uuid, 'file', uuid))
                process_info.derived_files[uuid] = derived_file

            for derived_biomaterial in derived_biomaterials:
                uuid = derived_biomaterial['uuid']['uuid']
                process_info.links.append(self.build_link_obj(process_name, process_uuid, 'biomaterial', uuid))

        for derived_by_process in derived_by_processes:
            self.recurse_process(derived_by_process, process_info)

    def get_input_bundle(self, process):
        bundle_manifests = list(self.ingest_api.getRelatedEntities('inputBundleManifests', process, 'bundleManifests'))

        if len(bundle_manifests) > 0:
            return bundle_manifests[0]

        return None

    def prepare_metadata_files(self, metadata_info) -> 'dict':
        metadata_files_by_type = dict()

        for entity_type in ['biomaterial', 'file', 'project', 'protocol', 'process']:
            metadata_files_by_type[entity_type] = list()
            specific_types_counter = dict()
            for (metadata_uuid, doc) in metadata_info[entity_type].items():
                specific_entity_type = self.get_concrete_entity_type(doc)
                specific_types_counter[specific_entity_type] = 0 if specific_entity_type not in specific_types_counter else specific_types_counter[specific_entity_type] + 1

                file_name = '{0}_{1}.json'.format(specific_entity_type, specific_types_counter[specific_entity_type])
                upload_filename = '{0}_{1}.json'.format(specific_entity_type, metadata_uuid)

                prepared_doc = {
                    'content': self.bundle_metadata(doc, metadata_uuid),
                    'content_type': '"metadata/{0}"'.format(entity_type),
                    'indexed': True,
                    'dss_filename': file_name,
                    'dss_uuid': metadata_uuid,
                    'upload_filename': upload_filename
                }

                metadata_files_by_type[entity_type].append(prepared_doc)

        return metadata_files_by_type

    def bundle_links(self, links):
        return {
            'describedBy': 'https://schema.humancellatlas.org/bundle/1.0.0/links',
            'schema_type': 'link_bundle',
            'schema_version': '1.0.0',
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

    def put_files_in_dss(self, bundle_uuid, files_to_put):
        created_files = []

        for bundle_file in files_to_put:
            version = ''

            try:
                created_file = self.dss_api.put_file(bundle_uuid, bundle_file)
                version = created_file['version']
            except Exception as e:
                raise FileDSSError('An error occurred while putting file in DSS' + str(e))

            file_param = {
                "indexed": bundle_file["indexed"],
                "name": bundle_file["submittedName"],
                "uuid": bundle_file["dss_uuid"],
                "content-type": bundle_file["content-type"],
                "version": version
            }

            created_files.append(file_param)

        return created_files

    def get_metadata_files(self, metadata_files_info):
        metadata_files = []

        for entity_type in ['biomaterial', 'file', 'project', 'protocol', 'process', 'links']:
            for metadata_file in metadata_files_info[entity_type]:
                metadata_files.append({
                    'name': metadata_file['upload_filename'],
                    'submittedName': metadata_file['dss_filename'],
                    'url': metadata_file['upload_file_url'],
                    'dss_uuid': metadata_file['dss_uuid'],
                    'indexed': metadata_file['indexed'],
                    'content-type': metadata_file['content_type']
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


    def create_bundle_manifest(self, submission_uuid):
        bundle_manifest = ingestapi.BundleManifest()
        bundle_manifest.envelopeUuid = submission_uuid

        return bundle_manifest


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

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = OptionParser()
    parser.add_option("-e", "--subsEnvUuid", dest="submissionsEnvelopeUuid",
                      help="Submission envelope UUID for which to generate the bundle")
    parser.add_option("-p", "--processUrl", dest="processUrl",
                      help="Process Url")
    parser.add_option("-D", "--dry", help="do a dry run without submitting to ingest", action="store_true",
                      default=False)
    parser.add_option("-o", "--output", dest="output",
                      help="output directory where to dump json files submitted to ingest", metavar="FILE",
                      default=None)
    parser.add_option("-i", "--ingest", help="the URL to the ingest API")
    parser.add_option("-s", "--staging", help="the URL to the staging API")
    parser.add_option("-d", "--dss", help="the URL to the datastore service")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')
    parser.add_option("-v", "--version", dest="schema_version", help="Metadata schema version", default=None)

    (options, args) = parser.parse_args()

    if not options.submissionsEnvelopeUuid:
        print("You must supply a submission envelope UUID")
        exit(2)

    if not options.processUrl:
        print("You must supply a processUrl")
        exit(2)

    ex = IngestExporter(options)

    ex.export_bundle(options.submissionsEnvelopeUuid, options.processUrl)
