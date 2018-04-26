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
        self.bundle_validator = bundlevalidator.BundleValidator()

    def writeBundleToFile(self, name, index, type, doc):
        dir = os.path.abspath("bundles/" + name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        bundleDir = os.path.abspath(dir + "/bundle" + index)
        if not os.path.exists(bundleDir):
            os.makedirs(bundleDir)
        tmpFile = open(bundleDir + "/" + type + ".json", "w")
        tmpFile.write(json.dumps(self.bundleProject(doc), indent=4))
        tmpFile.close()

    def getSchemaNameForEntity(self, schemaUrl):
        return schemaUrl["content"]["describedBy"].rsplit('/', 1)[-1]

    def getLinks(self, source_type, source_id, destination_type, destination_id):
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


    def bundleFileIngest(self, file_entity):
        return self._bundleEntityIngest(file_entity)

    def bundleProtocolIngest(self, protocol_entity):
        return self._bundleEntityIngest(protocol_entity)

    def _bundleEntityIngest(self, entity):
        return {
            'content': entity['content'],
            'hca_ingest': {
                'document_id': entity['uuid']['uuid'],
                'submissionDate': entity['submissionDate']
            }
        }

    def writeMetadataToStaging(self, submissionId, fileName, content, contentType):
        self.logger.info("writing to staging area..." + fileName)
        fileDescription = self.staging_api.stageFile(submissionId, fileName, content, contentType)
        self.logger.info("File staged at " + fileDescription.url)
        return fileDescription

    def bundleSample(self, sample_entity):
        sample_copy = self._copyAndTrim(sample_entity)
        bundle = {
            'content': sample_copy.pop('content', None),
            'hca_ingest': sample_copy
        }

        bundle["hca_ingest"]["document_id"] = bundle["hca_ingest"]["uuid"]["uuid"]
        del bundle["hca_ingest"]["uuid"]

        if bundle["hca_ingest"]["accession"] is None:
            bundle["hca_ingest"]["accession"] = ""
        return bundle

    def bundleProcess(self, process_entity):
        process_copy = self._copyAndTrim(process_entity)
        bundle = {
            'content': process_copy.pop('content', None),
            'hca_ingest': process_copy
        }

        bundle["hca_ingest"]["document_id"] = bundle["hca_ingest"]["uuid"]["uuid"]
        del bundle["hca_ingest"]["uuid"]

        if bundle["hca_ingest"]["accession"] is None:
            bundle["hca_ingest"]["accession"] = ""
        return bundle

    def bundleProject(self, project_entity):
        project_copy = self._copyAndTrim(project_entity)
        bundle = {
            'describedBy': "https://schema.humancellatlas.org/bundle/5.1.0/project",
            'schema_version': "5.1.0",
            'schema_type': 'project_bundle',
            'content': project_copy.pop('content', None),
            'hca_ingest': project_copy
        }

        bundle["hca_ingest"]["document_id"] = bundle["hca_ingest"]["uuid"]["uuid"]

        del bundle["hca_ingest"]["uuid"]

        if bundle["hca_ingest"]["accession"] is None:
            bundle["hca_ingest"]["accession"] = ""
        return bundle

    def bundleProtocol(self, protocol_entity):
        protocol_copy = self._copyAndTrim(protocol_entity)
        bundle = {
            'content': protocol_copy.pop('content', None),
            'hca_ingest': protocol_copy
        }

        bundle["hca_ingest"]["document_id"] = bundle["hca_ingest"]["uuid"]["uuid"]
        del bundle["hca_ingest"]["uuid"]

        if bundle["hca_ingest"]["accession"] is None:
            bundle["hca_ingest"]["accession"] = ""
        return bundle

    def _copyAndTrim(self, project_entity):
        copy = project_entity.copy()
        for property in ["_links", "events", "validationState", "validationErrors", "user", "lastModifiedUser"]:
            if property in copy:
                del copy[property]
        return copy

    def dumpJsonToFile(self, object, projectId, name, output_dir=None):
        if output_dir:
            self.outputDir = output_dir

        if self.outputDir:
            dir = os.path.abspath(self.outputDir)
            if not os.path.exists(dir):
                os.makedirs(dir)
            tmpFile = open(dir + "/" + projectId + "_" + name + ".json", "w")
            tmpFile.write(json.dumps(object, indent=4))
            tmpFile.close()

    def export_bundle(self, submission_uuid, process_url):
        self.logger.info('Export bundle for process: ' + process_url)

        saved_bundle_uuid = None

        self.logger.info('Retrieving all process information...')
        process_info = self.get_all_process_info(process_url)

        self.logger.info('Generating bundle files...')
        metadata_files_info = self.prepare_metadata_files(process_info)
        data_files_info = self.prepare_data_files(process_info)

        bundle_manifest = self.create_bundle_manifest(submission_uuid, metadata_files_info, process_info)

        if self.dryrun:
            self.logger.info('Export is using dry run mode.')
            self.logger.info('Dumping bundle files...')
            self.dump_metadata_files_and_bundle_manifest(metadata_files_info, bundle_manifest)

        else:
            self.logger.info('Uploading metadata files...')
            self.upload_metadata_files(submission_uuid, metadata_files_info)

            self.logger.info('Saving files in DSS...')
            bundle_uuid = bundle_manifest.bundleUuid

            metadata_files = self.get_metadata_files(metadata_files_info)
            data_files = self.get_data_files(data_files_info)
            bundle_files = metadata_files + data_files

            created_files = self.put_files_in_dss(bundle_uuid, bundle_files)

            self.logger.info('Saving bundle in DSS...')
            self.put_bundle_in_dss(bundle_uuid, created_files)

            self.logger.info('Saving bundle manifest...')
            self.ingest_api.createBundleManifest(bundle_manifest)

            saved_bundle_uuid = bundle_manifest.bundleUuid

            self.logger.info('Bundle ' + saved_bundle_uuid + ' was successfully created!')

        return saved_bundle_uuid

    def get_all_process_info(self, process_url):
        process = self.ingest_api.getAssay(process_url)  # TODO rename getAssay to getProcess
        process_info = ProcessInfo()
        process_info.input_bundle = self.get_input_bundle(process)

        process_info.project = self.get_project_info(process)

        if not process_info.project:  # get from input bundle
            project_uuid_lists = process_info.input_bundle['fileProjectMap'].values()

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
            process_name = self.getSchemaNameForEntity(process_to_link)
            process_uuid = process_to_link['uuid']['uuid']

            for input_biomaterial in input_biomaterials:
                uuid = input_biomaterial['uuid']['uuid']
                process_info.links.append(self.getLinks('biomaterial', uuid, process_name, process_uuid))

            for input_file in input_files:
                uuid = input_file['uuid']['uuid']
                process_info.links.append(self.getLinks('file', uuid, process_name, process_uuid))

            protocols = list(self.ingest_api.getRelatedEntities('protocols', process_to_link, 'protocols'))
            for protocol in protocols:
                uuid = protocol['uuid']['uuid']
                process_info.links.append(self.getLinks(process_name, process_uuid, 'protocol', uuid))
                process_info.protocols[uuid] = protocol

            for derived_file in derived_files:
                uuid = derived_file['uuid']['uuid']
                process_info.links.append(self.getLinks(process_name, process_uuid, 'file', uuid))
                process_info.derived_files[uuid] = derived_file

            for derived_biomaterial in derived_biomaterials:
                uuid = derived_biomaterial['uuid']['uuid']
                process_info.links.append(self.getLinks(process_name, process_uuid, 'biomaterial', uuid))

        for derived_by_process in derived_by_processes:
            self.recurse_process(derived_by_process, process_info)

    def get_input_bundle(self, process):
        bundle_manifests = list(self.ingest_api.getRelatedEntities('inputBundleManifests', process, 'bundleManifests'))

        if len(bundle_manifests) > 0:
            return bundle_manifests[0]

        return None

    def prepare_metadata_files(self, process_info):
        bundle_content = self.build_and_validate_content(process_info)

        metadata_files = {}

        file_uuid = str(uuid.uuid4())
        metadata_files['project'] = {
            'content': bundle_content['project'],
            'content_type': '"metadata/project"',
            'indexed': True,
            'dss_filename': 'project.json',
            'dss_uuid': file_uuid,
            'upload_filename': 'project_bundle_' + file_uuid + '.json'
        }

        file_uuid = str(uuid.uuid4())
        metadata_files['biomaterial'] = {
            'content': bundle_content['biomaterial'],
            'content_type': '"metadata/biomaterial"',
            'indexed': True,
            'dss_filename': 'biomaterial.json',
            'dss_uuid': file_uuid,
            'upload_filename': 'biomaterial_bundle_' + file_uuid + '.json'
        }

        file_uuid = str(uuid.uuid4())
        metadata_files['process'] = {
            'content': bundle_content['process'],
            'content_type': '"metadata/process"',
            'indexed': True,
            'dss_filename': 'process.json',
            'dss_uuid': file_uuid,
            'upload_filename': 'process_bundle_' + file_uuid + '.json'
        }

        file_uuid = str(uuid.uuid4())
        metadata_files['protocol'] = {
            'content': bundle_content['protocol'],
            'content_type': '"metadata/protocol"',
            'indexed': True,
            'dss_filename': 'protocol.json',
            'dss_uuid': file_uuid,
            'upload_filename': 'protocol_bundle_' + file_uuid + '.json'
        }

        file_uuid = str(uuid.uuid4())
        metadata_files['file'] = {
             'content': bundle_content['file'],
             'content_type': '"metadata/file"',
             'indexed': True,
             'dss_filename': 'file.json',
             'dss_uuid': file_uuid,
             'upload_filename': 'file_bundle_' + file_uuid + '.json'
        }

        file_uuid = str(uuid.uuid4())
        metadata_files['links'] = {
            'content': bundle_content['links'],
            'content_type': '"metadata/links"',
            'indexed': True,
            'dss_filename': 'links.json',
            'dss_uuid': file_uuid,
            'upload_filename': 'links_bundle_' + file_uuid + '.json'
        }

        self._inherit_same_files_from_input(metadata_files, process_info)

        return metadata_files

    # if new file has same set of uuids as the input bundle file
    # do not re-upload or create bundle metadata file in dss
    # reuse the file uuid
    # this scenario might only happen for project, biomaterial, protocol map
    def _inherit_same_files_from_input(self, metadata_files, process_info):
        input_bundle = process_info.input_bundle

        if not input_bundle:
            return

        file_uuids = [process_info.project['uuid']['uuid']]
        input_file = self._compare_to_input_file(input_bundle, 'fileProjectMap', file_uuids)
        if input_file['is_equal']:
            metadata_files['project']['dss_uuid'] = input_file['file_uuid']
            metadata_files['project']['is_same_as_input'] = input_file['is_equal']

        file_uuids = process_info.input_biomaterials.keys()
        input_file = self._compare_to_input_file(input_bundle, 'fileBiomaterialMap', file_uuids)
        if input_file['is_equal']:
            metadata_files['biomaterial']['dss_uuid'] = input_file['file_uuid']
            metadata_files['biomaterial']['is_same_as_input'] = input_file['is_equal']

        file_uuids = process_info.protocols.keys()
        input_file = self._compare_to_input_file(input_bundle, 'fileProtocolMap', file_uuids)
        if input_file['is_equal']:
            metadata_files['protocol']['dss_uuid'] = input_file['file_uuid']
            metadata_files['protocol']['is_same_as_input'] = input_file['is_equal']

    def _compare_to_input_file(self, input_bundle, attr, file_uuids):
        file_map = input_bundle[attr]
        input_file_uuids = list(file_map.values())[0]
        input_file_uuid = list(file_map.keys())[0]

        is_same_as_input_file = self._are_equal_lists(input_file_uuids, file_uuids)

        return {
            'is_equal': is_same_as_input_file,
            'file_uuid': input_file_uuid
        }

    # compare two lists ignoring order
    def _are_equal_lists(self, list1, list2):
        set1 = frozenset(list(list1))
        set2 = frozenset(list(list2))
        diff = set1.difference(set2)

        return not len(diff)

    # build bundle json for each entity according to schema
    def build_and_validate_content(self, process_info):
        bundle_contents = {
            'project': self.bundleProject(process_info.project),
            'biomaterial': self.bundle_biomaterials(process_info.input_biomaterials.values()),
            'process': self.bundle_processes(process_info.derived_by_processes.values()),
            'file': self.bundle_files(list(process_info.input_files.values()) + list(process_info.derived_files.values())),
            'protocol': self.bundle_protocols(process_info.protocols.values()),
            'links': self.bundle_links(process_info.links)
        }

        # TODO comment out for now, validation seems broken
        # self.validate_metadata_files(bundle_contents)

        return bundle_contents

    def bundle_biomaterials(self, biomaterials):
        return {
            'describedBy': 'https://schema.humancellatlas.org/bundle/5.1.0/biomaterial',
            'schema_version': '5.1.0',
            'schema_type': 'biomaterial_bundle',
            'biomaterials': list(map(self.bundleSample, biomaterials))
        }

    def bundle_processes(self, processes):
        return {
            'describedBy': 'https://schema.humancellatlas.org/bundle/5.2.1/process',
            'schema_version': '5.2.1',
            'schema_type': 'process_bundle',
            'processes': list(map(self.bundleProcess, processes))
        }

    def bundle_files(self, files):
        return {
            'describedBy': 'https://schema.humancellatlas.org/bundle/1.0.0/file',
            'schema_version': '1.0.0',
            'schema_type': 'file_bundle',
            'files': list(map(self.bundleFileIngest, files))
        }

    def bundle_protocols(self, protocols):
        return {
            'describedBy': 'https://schema.humancellatlas.org/bundle/5.1.0/protocol',
            'schema_type': 'protocol_bundle',
            'schema_version': '5.1.0',
            'protocols': list(map(self.bundleProtocol, protocols))
        }

    def bundle_links(self, links):
        return {
            'describedBy': 'https://schema.humancellatlas.org/bundle/1.0.0/links',
            'schema_type': 'link_bundle',
            'schema_version': '1.0.0',
            'links': links
        }

    def validate_metadata_files(self, bundle_contents):
        schema_version = {
            'project': None,
            'biomaterial': None,
            'process': None,
            'protocol': None,
            'file': '1.0.0',
            'links': '1.0.0'
        }

        for metadata_type, content in bundle_contents.items():
            # TODO fix bundle validator to only return the validation report
            result = self.bundle_validator.validate(content, metadata_type, schema_version[metadata_type])
            if not (True == result):
                raise InvalidBundleError('Invalid ' + metadata_type + ' bundle!')

    def create_bundle_manifest(self, submission_uuid, metadata_files_info, process_info):
        bundle_manifest = ingestapi.BundleManifest()

        bundle_manifest.envelopeUuid = submission_uuid

        bundle_manifest.dataFiles = list(process_info.derived_files.keys())

        bundle_manifest.fileProjectMap = {
            metadata_files_info['project']['dss_uuid']: list([process_info.project["uuid"]["uuid"]])
        }

        bundle_manifest.fileBiomaterialMap = {
            metadata_files_info['biomaterial']['dss_uuid']: list(process_info.input_biomaterials.keys())
        }

        bundle_manifest.fileProcessMap = {
            metadata_files_info['process']['dss_uuid']: list(process_info.derived_by_processes.keys())
        }

        bundle_manifest.fileProtocolMap = {
            metadata_files_info['protocol']['dss_uuid']: list(process_info.protocols.keys())
        }

        bundle_manifest.fileFilesMap = {
            metadata_files_info['file']['dss_uuid']: list(process_info.derived_files.keys())
        }

        # TODO store the version timestamp in bundle manifest
        # TODO do we need a fileLinksMap mapping in the bundle manifest?

        return bundle_manifest

    def dump_metadata_files_and_bundle_manifest(self, metadata_files_info, bundle_manifest):
        project = metadata_files_info['project']['content']
        project_keyword = project['content']['project_core']['project_shortname']

        for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
            bundle_file = metadata_files_info[metadata_type]
            self.dumpJsonToFile(bundle_file['content'], project_keyword, metadata_type + '_bundle')

        self.dumpJsonToFile(bundle_manifest.__dict__, project_keyword, 'bundleManifest')

    def upload_metadata_files(self, submission_uuid, metadata_files_info):
        try:
            for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
                bundle_file = metadata_files_info[metadata_type]
                filename = bundle_file['upload_filename']
                content = bundle_file['content']
                content_type = bundle_file['content_type']

                uploaded_file = self.writeMetadataToStaging(submission_uuid, filename, content, content_type)
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
                if bundle_file['is_same_as_input']:
                    # TODO this assumes that the latest version is the file version in the input bundle, should be a safe assumption for now
                    # Ideally, bundle manifest must store the file uuid and version and version must be retrieved from there
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
                "uuid": bundle_file["dss_uuid"],
                "content-type": bundle_file["content-type"],
                "version": version
            }

            created_files.append(file_param)

        return created_files

    def get_metadata_files(self, metadata_files_info):
        metadata_files = []

        for metadata_type, metadata_file in metadata_files_info.items():
            metadata_files.append({
                'name': metadata_file['upload_filename'],
                'submittedName': metadata_file['dss_filename'],
                'url': metadata_file['upload_file_url'],
                'dss_uuid': metadata_file['dss_uuid'],
                'indexed': metadata_file['indexed'],
                'content-type': metadata_file['content_type'],
                'is_same_as_input': metadata_file['is_same_as_input'] if 'is_same_as_input' in metadata_file else False
            })

        return metadata_files

    def get_data_files(self, derived_files):
        data_files = []

        for uuid, data_file in derived_files.items():
            filename = data_file['fileName']
            cloud_url = data_file['cloudUrl']

            data_files.append({
                'name': filename,
                'submittedName': filename,
                'url': cloud_url,
                'dss_uuid': uuid,
                'indexed': False,
                'content-type': 'data',
                'is_same_as_input': data_file['is_same_as_input']
            })

        return data_files

    def prepare_data_files(self, process_info):
        for uuid, data_file in process_info.derived_files.items():
            process_info.derived_files[uuid]['is_same_as_input'] = uuid in process_info.input_files

        return process_info.derived_files

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
        print ("You must supply a submission envelope UUID")
        exit(2)

    if not options.processUrl:
        print ("You must supply a processUrl")
        exit(2)

    ex = IngestExporter(options)

    ex.export_bundle(options.submissionsEnvelopeUuid, options.processUrl)
