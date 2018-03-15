#!/usr/bin/env python
"""
desc goes here
"""
from dssapi import DssApi
from stagingapi import StagingApi

__author__ = "jupp"
__license__ = "Apache 2.0"

import logging
import ingestapi
import json
import uuid
import time
import requests
from optparse import OptionParser
import os, sys
import time
from stagingapi import StagingApi
from bundlevalidator import BundleValidator

DEFAULT_INGEST_URL=os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL=os.environ.get('STAGING_API', 'http://staging.dev.data.humancellatlas.org')
DEFAULT_DSS_URL=os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')

BUNDLE_SCHEMA_BASE_URL=os.environ.get('BUNDLE_SCHEMA_BASE_URL', 'https://schema.humancellatlas.org/bundle/%s/')
METADATA_SCHEMA_VERSION = os.environ.get('SCHEMA_VERSION', '5.1.0')


class IngestExporter:
    def __init__(self, options={}):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        self.logger = logging.getLogger(__name__)

        self.dryrun = options.dry if options and options.dry else False
        self.outputDir = options.output if options and options.output else None

        self.ingestUrl = options.ingest if options and options.ingest else os.path.expandvars(DEFAULT_INGEST_URL)
        self.stagingUrl = options.staging if options and options.staging else os.path.expandvars(DEFAULT_STAGING_URL)
        self.dssUrl = options.dss if options and options.dss else os.path.expandvars(DEFAULT_DSS_URL)
        self.schema_version = options.schema_version if options and options.schema_version else os.path.expandvars(METADATA_SCHEMA_VERSION)
        self.schema_url = os.path.expandvars(BUNDLE_SCHEMA_BASE_URL % self.schema_version)

        self.logger.debug("ingest url is "+self.ingestUrl)

        self.staging_api = StagingApi()
        self.dss_api = DssApi()
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)
        self.bundle_validator = BundleValidator()

    def writeBundleToFile(self, name, index, type, doc):
        dir = os.path.abspath("bundles/"+name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        bundleDir = os.path.abspath(dir+"/bundle"+index)
        if not os.path.exists(bundleDir):
            os.makedirs(bundleDir)
        tmpFile = open(bundleDir + "/"+type+".json", "w")
        tmpFile.write(json.dumps(self.bundleProject(doc), indent=4))
        tmpFile.close()


    def getNestedObjects(self, relation, entity, entityType):
        nestedChildren = []
        for nested in self.ingest_api.getRelatedEntities(relation, entity, entityType):
            # children = self.getNestedObjects(relation, nested, entityType)
            # if len(children) > 0:
            #     nested["content"][relation] = children
            nestedChildren.append(nested)
            # for child in children:
            #     nestedChildren.append(self.getBundleDocument(child))
        return nestedChildren

    def buildSampleObject(self, sample, nestedSamples):
        nestedProtocols = self.getNestedObjects("protocols", sample, "protocols")

        primarySample = self.bundleProject(sample)
        primarySample["derivation_protocols"] = []

        for p, protocol in enumerate(nestedProtocols):
            primarySample["derivation_protocols"].append(nestedProtocols[p]["content"])

        if nestedSamples:
            primarySample["derived_from"] = nestedSamples[0]["uuid"]["uuid"]

        return primarySample

    def getLinks(self, source_type, source_id, destination_type, destination_id):
        return {
            'source_type' : source_type,
            'source_id' : source_id,
            'destination_type' : destination_type,
            'destination_id' : destination_id
        }


    def generateBundles(self, submissionEnvelopeId):
        self.logger.info('submission received '+submissionEnvelopeId)
        # given a sub envelope, generate bundles

        submissionUrl = self.ingest_api.getSubmissionUri(submissionId=submissionEnvelopeId)
        submissionUuid = self.ingest_api.getObjectUuid(submissionUrl)

        # check staging area is available
        if self.dryrun or self.staging_api.hasStagingArea(submissionUuid):

            assays = self.ingest_api.getAssays(submissionUrl)
            analyses = self.ingest_api.getAnalyses(submissionUrl)

            self.logger.info("Attempting to export primary submissions to DSS...")
            self.primarySubmission(submissionUuid,assays)

            self.logger.info("Attempting to export secondary submissions to DSS...")
            self.secondarySubmission(submissionUuid,analyses)

            if (not self.dryrun):
                # cleanup
                self.deleteStagingArea(submissionUuid)
        else:
            self.logger.error("Can\'t do export as no staging area has been created")

    def secondarySubmission(self, submissionEnvelopeUuid, analyses):
        # list of FileDescriptors for files we need to transfer to the DSS before creating the bundle
        filesToTransfer = []

        # generate the analysis.json
        # assume there's only 1 analysis metadata, TODO:  expand later...
        for index, analysis in enumerate(analyses):

            # get the referenced bundle manififest (assume there's only 1)
            inputBundle = list(self.ingest_api.getRelatedEntities("inputBundleManifests", analysis, "bundleManifests"))[0] # TODO: analysis only valid iff bundleManifests.length > 0 ?

            # the new bundle manifest === the old manifest (union) staged analysis file (union) new data files
            bundleManifest = self.makeCopyBundle(inputBundle)
            bundleManifest.envelopeUuid = submissionEnvelopeUuid

            # add the referenced files to the bundle manifest and to the files to transfer
            files = list(self.ingest_api.getRelatedEntities("files", analysis, "files"))
            bundleManifest.files += list(map(lambda file_json : file_json["uuid"]["uuid"], files))
            filesToTransfer += list(map(lambda file_json: {"name": file_json["fileName"],
                                                           "submittedName": file_json["fileName"],
                                                           "url": file_json["cloudUrl"],
                                                           "dss_uuid": file_json["uuid"]["uuid"],
                                                           "indexed" : False,
                                                           "content-type": "data"
                                                           }, files))

            # stage the analysis.json, add to filesToTransfer and to the bundle manifest
            analysisUuid = analysis["uuid"]["uuid"]
            analysisDssUuid = unicode(uuid.uuid4())
            analysisBundleContent = self.bundleProject(analysis)
            analysisFileName = "analysis_0.json" # TODO: shouldn't be hardcoded

            analysisBundleContent["core"] = {"type": "analysis_bundle",
                                            "schema_url": self.schema_url + "analysis_bundle.json",
                                            "schema_version": self.schema_version}

            bundleManifest.fileAnalysisMap = { analysisDssUuid : [analysisUuid] }

            if not self.dryrun:
                fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, analysisFileName, analysisBundleContent, "\"metadata/analysis\"")
                filesToTransfer.append({"name":analysisFileName,
                                        "submittedName":"analysis.json",
                                        "url":fileDescription.url,
                                        "dss_uuid": analysisDssUuid,
                                        "indexed" : True,
                                        "content-type": "hca-analysis"})

                # generate new bundle
                # write to DSS
                self.dss_api.createAnalysisBundle(inputBundle, bundleManifest, filesToTransfer)

                # write bundle manifest to ingest API
                self.ingest_api.createBundleManifest(bundleManifest)

            else:
                valid = self.bundle_validator.validate(analysisBundleContent, "analysis")
                if valid:
                    self.logger.info("Assay entity " + analysisDssUuid + " is valid")
                else:
                    self.logger.info("Assay entity " + analysisDssUuid + " is not valid")
                    self.logger.info(valid)

                self.dumpJsonToFile(analysisBundleContent, analysisBundleContent["content"]["analysis_id"], "analysis_bundle_" + str(index))
                self.dumpJsonToFile(bundleManifest.__dict__, analysisBundleContent["content"]["analysis_id"], "bundleManifest_" + str(index))



    def primarySubmission(self, submissionEnvelopeUuid, assays):

        # we only want to upload one version of each file so must track through each bundle files that are the same e.g. project and possibly protocols or samples

        projectUuidToBundleData = {}
        sampleUuidToBundleData = {}
        processUuidToBundleData = {}
        protocolUuidToBundleData = {}
        fileUuidToBundleData = {}
        dataUuidToBundleData = {}

        # create a bundle per assay

        for index, assay in enumerate(assays):

            # catch links for this bundle
            links = []

            # collect all protocol, biomaterial, file and process ids, these will go in to bundle manifest
            allBiomaterialUuids = []
            allProcessUuids = []
            allFileUuids = []
            allProtocolUuids = []

            allBundleFilesToSubmit = []

            # create the bundle manifest to track file uuid to object uuid maps for this bundle
            bundleManifest = ingestapi.BundleManifest()
            bundleManifest.envelopeUuid = submissionEnvelopeUuid

            # get the project and create the project bundle
            projectEntities = list(self.ingest_api.getRelatedEntities("projects", assay, "projects"))
            if len(projectEntities) != 1:
                raise ValueError("Can only be one project in bundle")

            project = projectEntities[0]
            project_bundle = self.bundleProject(project)

            # track the file names we have uploaded to staging based on uuid
            # this avoids duplicating files on staging
            projectUuid = project["uuid"]["uuid"]
            if projectUuid not in projectUuidToBundleData:
                projectDssUuid = str(uuid.uuid4())
                projectFileName = "project_bundle_"+str(index)+".json"

                if not self.dryrun:
                    fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, projectFileName, project_bundle, '"metadata/project"')
                    projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":fileDescription.url, "dss_uuid": projectDssUuid, "indexed": True, "content-type" : '"metadata/project"'}
                else:
                    projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":"", "dss_uuid": projectDssUuid, "indexed": True, "content-type" : '"metadata/project"'}
                    valid = self.bundle_validator.validate(project_bundle, "project")
                    if valid:
                        self.logger.info("Project entity " + projectDssUuid + " is valid")
                    else:
                        self.logger.info("Project entity " + projectDssUuid + " is not valid")
                        self.logger.info(valid)
                    self.dumpJsonToFile(project_bundle, project_bundle["content"]["project_core"]["project_shortname"],
                                        "project_bundle_" + str(index))

                bundleManifest.fileProjectMap = {projectDssUuid: [projectUuid]}

            else:
                bundleManifest.fileProjectMap = {projectUuidToBundleData[projectUuid]["dss_uuid"]: [projectUuid]}

            allBundleFilesToSubmit.append(projectUuidToBundleData[projectUuid])


            # create a stub for the biomaterial bundle
            biomaterialBundle = {
                'describedBy': 'https://schema.humancellatlas.org/bundle/5.1.0/biomaterial',
                'schema_version': '5.1.0',
                'schema_type': 'biomaterial_bundle',
                'biomaterials': []
            }

            # create a stub for the process bundle
            processesBundle = {
                'describedBy': 'https://schema.humancellatlas.org/bundle/5.2.1/process',
                'schema_version': '5.2.1',
                'schema_type': 'process_bundle',
                'processes': []
            }

            # create a stub for the file bundle
            fileBundle = {
                    'describedBy': 'https://schema.humancellatlas.org/bundle/1.0.0/file',
                    'schema_version': '1.0.0',
                    'schema_type': 'file_bundle',
                    'files': []
            }

            # create a stub for the protocol bundle
            protocolBundle = {
                'describedBy': 'https://schema.humancellatlas.org/bundle/5.1.0/protocol',
                'schema_type': 'protocol_bundle',
                'schema_version': '5.1.0',
                'protocols': []
            }

            # create a stub for the links bundle
            linksBundle = {
                'describedBy': 'https://schema.humancellatlas.org/bundle/1.0.0/links',
                'schema_type': 'link_bundle',
                'schema_version': '1.0.0',
                'links': []
            }


            # get the primary sample for this assay
            # assume just one input sample todo this is not a safe assumption
            input_sample = list(self.ingest_api.getRelatedEntities("inputBiomaterials", assay, "biomaterials"))[0]
            biomaterialBundle["biomaterials"].append(self.bundleSample(input_sample))
            assaySampleUuid = input_sample["uuid"]["uuid"]
            allBiomaterialUuids.append(assaySampleUuid)

            # This section here is where we find all the related samples
            sampleProcesses = self.getNestedObjects("derivedByProcesses", input_sample, "processes")

            for sampleProcess in sampleProcesses:

                # collect this process uuid and add the process to the bundle
                if (sampleProcess["uuid"]["uuid"] not in allProcessUuids):
                    allProcessUuids.append(sampleProcess["uuid"]["uuid"])
                    processesBundle["processes"].append(self.bundleProcess(sampleProcess))

                # get the sample input to this porcess, this will be the specimen
                specimenSamples = self.getNestedObjects("inputBiomaterials", sampleProcess, "biomaterials")
                for specimen in specimenSamples:


                    # collect the sample uuid and add it to the sample bundle
                    if (specimen["uuid"]["uuid"] not in allBiomaterialUuids):
                        allBiomaterialUuids.append(specimen["uuid"]["uuid"])
                        biomaterialBundle["biomaterials"].append(self.bundleSample(specimen))

                        # link the samples
                        links.append(self.getLinks("biomaterial",specimen["uuid"]["uuid"] , "biomaterial",assaySampleUuid ))



                    # get the process that generated the specimen
                    specimenProcesses = self.getNestedObjects("derivedByProcesses", specimen, "processes")
                    for specimenProcess in specimenProcesses:

                        # collect the process and create add it to the process bundle
                        if (specimenProcess["uuid"]["uuid"] not in allProcessUuids):
                            allProcessUuids.append(specimenProcess["uuid"]["uuid"])
                            processesBundle["processes"].append(self.bundleProcess(specimenProcess))

                        # finally get the donor and add the sample bundle
                        donorSamples = self.getNestedObjects("inputBiomaterials", specimenProcess, "biomaterials")

                        for donor in donorSamples:
                            if (donor["uuid"]["uuid"] not in allBiomaterialUuids):
                                allBiomaterialUuids.append(donor["uuid"]["uuid"])
                                biomaterialBundle["biomaterials"].append(self.bundleSample(donor))
                                # link the samples
                                links.append(
                                self.getLinks("biomaterial", donor["uuid"]["uuid"], "biomaterial",
                                              specimenProcess["uuid"]["uuid"]))


            # now submit the samples to the DSS
            if assaySampleUuid not in sampleUuidToBundleData:
                sampleDssUuid = str(uuid.uuid4())
                sampleFileName = "biomaterial_bundle_"+str(index)+".json"

                if not self.dryrun:
                    fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, sampleFileName, biomaterialBundle, '"metadata/biomaterial"')
                    sampleUuidToBundleData[assaySampleUuid] = {"name":sampleFileName, "submittedName":"biomaterial.json", "url":fileDescription.url, "dss_uuid": sampleDssUuid, "indexed": True, "content-type" : '"metadata/sample"'}
                else:
                    sampleUuidToBundleData[assaySampleUuid] = {"name":sampleFileName, "submittedName":"biomaterial.json", "url":"", "dss_uuid": sampleDssUuid, "indexed": True, "content-type" : '"metadata/sample"'}
                    valid = self.bundle_validator.validate(biomaterialBundle, "biomaterial")
                    if valid:
                        self.logger.info("Biomaterial entity " + sampleDssUuid + " is valid")
                    else:
                        self.logger.info("Biomaterial entity " + sampleDssUuid + " is not valid")
                        self.logger.info(valid)
                    self.dumpJsonToFile(biomaterialBundle, project_bundle["content"]["project_core"]["project_shortname"], "biomaterial_bundle_" + str(index))

                bundleManifest.fileBiomaterialMap = {sampleDssUuid: allBiomaterialUuids}
            # else add any new sampleUuids to the related samples list
            else:
               bundleManifest.fileBiomaterialMap = {sampleUuidToBundleData[assaySampleUuid]["dss_uuid"]: allBiomaterialUuids}

            allBundleFilesToSubmit.append(sampleUuidToBundleData[assaySampleUuid])

            # push the data file metadata and create a file bundle
            fileDssUuid = str(uuid.uuid4())
            fileBundleFileName = "file_bundle_" + str(index) + ".json"

            for file in self.ingest_api.getRelatedEntities("derivedFiles", assay, "files"):

                # add the file to the file bundle
                fileIngest = self.bundleFileIngest(file)
                fileBundle["files"].append(fileIngest)

                fileUuid = file["uuid"]["uuid"]
                allFileUuids.append(fileUuid)

                fileName = file["fileName"]
                cloudUrl = file["cloudUrl"]

                dataUuidToBundleData[fileUuid] = {"name":fileName, "submittedName":fileName, "url":cloudUrl, "dss_uuid": fileUuid, "indexed": False, "content-type" : "data"}
                allBundleFilesToSubmit.append(dataUuidToBundleData[fileUuid])
                bundleManifest.dataFiles.append(fileUuid)

                # link the samples
                links.append(
                self.getLinks("biomaterial", assaySampleUuid, "file",
                              fileUuid))

            # write the file bundle to the datastore
            if not self.dryrun:
                bundlefileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, fileBundleFileName, fileBundle, '"metadata/file"')
                allBundleFilesToSubmit.append({"name":fileBundleFileName, "submittedName":"file.json", "url":bundlefileDescription.url, "dss_uuid": fileDssUuid, "indexed": True, "content-type" : '"metadata/file"'})
            else:
                allBundleFilesToSubmit.append({"name":fileBundleFileName, "submittedName":"file.json", "url":"", "dss_uuid": fileDssUuid, "indexed": True, "content-type" : '"metadata/file"'})
                valid = self.bundle_validator.validate(fileBundle, "file", "1.0.0")
                if valid:
                    self.logger.info("File entity " + fileDssUuid + " is valid")
                else:
                    self.logger.info("File entity " + fileDssUuid + " is not valid")
                    self.logger.info(valid)

                self.dumpJsonToFile(fileBundle, project_bundle["content"]["project_core"]["project_shortname"], "file_bundle_" + str(index))

            bundleManifest.fileFilesMap = {fileDssUuid: allFileUuids}


            assayUuid = assay["uuid"]["uuid"]
            allProcessUuids.append(assayUuid)

            #TO DO: this is hack in v4 because the bundle schema is specified as an array rather than an object! this should be corrected in v5
            processesBundle["processes"].append(self.bundleProcess(assay))

            assayDssUuid = str(uuid.uuid4())
            assayFileName = "process_bundle_" + str(index) + ".json"

            if not self.dryrun:
                fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, assayFileName, processesBundle, '"metadata/process"')
                allBundleFilesToSubmit.append({"name":assayFileName, "submittedName":"process.json", "url":fileDescription.url, "dss_uuid": assayDssUuid, "indexed": True, "content-type" : '"metadata/process"'})
            else:
                allBundleFilesToSubmit.append({"name":assayFileName, "submittedName":"process.json", "url":"", "dss_uuid": assayDssUuid, "indexed": True, "content-type" : '"metadata/process"'})
                valid = self.bundle_validator.validate(processesBundle, "process")
                if valid:
                    self.logger.info("Process entity " + assayDssUuid + " is valid")
                else:
                    self.logger.info("Process entity " + assayDssUuid + " is not valid")
                    self.logger.info(valid)

                self.dumpJsonToFile(processesBundle, project_bundle["content"]["project_core"]["project_shortname"], "process_bundle_" + str(index))

            bundleManifest.fileProcessMap = {assayDssUuid: allProcessUuids}

            # push protocols to dss

            for protocol in list(self.ingest_api.getRelatedEntities("protocols", assay, "protocols")):
                protocol_ingest = self.bundleProtocol(protocol)
                protocolBundle['protocols'].append(protocol_ingest)

                allProtocolUuids.append(protocol["uuid"]["uuid"])


            protocolDssUuid = str(uuid.uuid4())
            protocolBundleFileName = "protocol_bundle_" + str(index) + ".json"

            if not self.dryrun:
                bundlefileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid,
                    protocolBundleFileName, protocolBundle, '"metadata/protocol"')
                allBundleFilesToSubmit.append({
                    "name": protocolBundleFileName,
                    "submittedName": "protocol.json",
                    "url": bundlefileDescription.url,
                    "dss_uuid": protocolDssUuid,
                    "indexed": True,
                    "content-type": '"metadata/file"'
                })
            else:
                allBundleFilesToSubmit.append({
                    "name": protocolBundleFileName,
                    "submittedName": "protocol.json",
                    "url": "",
                    "dss_uuid": protocolDssUuid,
                    "indexed": True,
                    "content-type": '"metadata/file"'})
                valid = self.bundle_validator.validate(protocolBundle, "protocol")
                if valid:
                    self.logger.info("Protocol entity " + protocolDssUuid + " is valid")
                else:
                    self.logger.info("Protocol entity " + protocolDssUuid + " is not valid")
                    self.logger.info(valid)
                self.dumpJsonToFile(protocolBundle, project_bundle["content"]["project_core"]["project_shortname"],
                    "protocol_bundle_" + str(index))

            bundleManifest.fileProjectMap = {protocolDssUuid: [allProtocolUuids]}

            # push links to dss
            linksBundleFileName = "link_bundle_" + str(index) + ".json"
            linksDssUuid = str(uuid.uuid4())

            linksBundle["links"] = links
            if not self.dryrun:
                bundlefileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid,
                                                                    linksBundleFileName, linksBundle, '"metadata/links"')
                allBundleFilesToSubmit.append({
                    "name": linksBundleFileName,
                    "submittedName": "links.json",
                    "url": bundlefileDescription.url,
                    "dss_uuid": linksDssUuid,
                    "indexed": True,
                    "content-type": '"metadata/links"'
                })
            else:
                allBundleFilesToSubmit.append({
                    "name": linksBundleFileName,
                    "submittedName": "links.json",
                    "url": "",
                    "dss_uuid": linksDssUuid,
                    "indexed": True,
                    "content-type": '"metadata/links"'})
                valid = self.bundle_validator.validate(linksBundle, "links", "1.0.0")
                if valid:
                    self.logger.info("Link entity " + linksDssUuid + " is valid")
                else:
                    self.logger.info("Link entity " + linksDssUuid + " is not valid")
                    self.logger.info(valid)
                self.dumpJsonToFile(linksBundle, project_bundle["content"]["project_core"]["project_shortname"],
                    "link_bundle_" + str(index))

            self.logger.info("All files staged...")


            if not self.dryrun:
                # write to DSS
                self.dss_api.createBundle(bundleManifest.bundleUuid, allBundleFilesToSubmit)
                # write bundle manifest to ingest API
                self.ingest_api.createBundleManifest(bundleManifest)

            else:
                self.dumpJsonToFile(bundleManifest.__dict__, project_bundle["content"]["project_core"]["project_shortname"], "bundleManifest_" + str(index))

            self.logger.info("bundles generated! "+bundleManifest.bundleUuid)

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

    def deleteStagingArea(self, stagingAreaId):
      self.logger.info("deleting staging area...." + stagingAreaId)
      self.staging_api.deleteStagingArea(stagingAreaId)

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
            'describedBy': self.schema_url + "project",
            'schema_version': self.schema_version,
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
            del copy[property]
        return copy

    # returns a copy of a bundle manifest JSON, but with a new bundleUuid
    def makeCopyBundle(self, bundleToCopy):
        newBundle = ingestapi.BundleManifest()

        newBundle.dataFiles = bundleToCopy["files"]
        newBundle.fileBiomaterialMap = bundleToCopy["fileSampleMap"]
        newBundle.fileProcessMap = bundleToCopy["fileAssayMap"]
        newBundle.fileProjectMap = bundleToCopy["fileProjectMap"]
        newBundle.fileProtocolMap = bundleToCopy["fileProtocolMap"]
        return newBundle

    def completeSubmission(self, submissionEnvelopeId):
        for i in range(1, 5):
            try:
                self.ingest_api.updateSubmissionState(submissionEnvelopeId, 'cleaning')
                self.logger.info('Submission status is CLEANING')
            except Exception:
                self.logger.info("failed to set state of submission {0} to Cleaning, retrying...".format(submissionEnvelopeId))
                time.sleep(1)

        for i in range(1, 5):
            try:
                self.ingest_api.updateSubmissionState(submissionEnvelopeId, 'complete')
                self.logger.info('Submission status is COMPLETE')
            except Exception:
                self.logger.info("failed to set state of submission {0} to Complete, retrying...".format(submissionEnvelopeId))
                time.sleep(1)

    def processSubmission(self, submissionEnvelopeId):
        self.ingest_api.updateSubmissionState(submissionEnvelopeId, 'processing')

    def dumpJsonToFile(self, object, projectId, name):
        if self.outputDir:
            dir = os.path.abspath(self.outputDir)
            if not os.path.exists(dir):
                os.makedirs(dir)
            tmpFile = open(dir + "/" + projectId + "_" + name + ".json", "w")
            tmpFile.write(json.dumps(object, indent=4))
            tmpFile.close()

class Submission:
    def __init__(self):
        self.transfer_service_version = "v0.0.1"
        self.submitted_files = []

class File:
    def __init__(self):
        self.name = ""
        self.content_type = ""
        self.size = ""
        self.id = ""
        self.checksums = {}

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = OptionParser()
    parser.add_option("-e", "--subsEnvId", dest="submissionsEnvelopeId",
                      help="Submission envelope ID for which to generate bundles")
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
    if not options.submissionsEnvelopeId:
        print ("You must supply a submission envelope ID")
        exit(2)

    ex = IngestExporter(options)
    ex.generateBundles(options.submissionsEnvelopeId)
