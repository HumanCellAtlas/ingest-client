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
import requests
from optparse import OptionParser
import os, sys
from stagingapi import StagingApi
from bundlevalidator import BundleValidator

DEFAULT_INGEST_URL=os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL=os.environ.get('STAGING_API', 'http://staging.dev.data.humancellatlas.org')
DEFAULT_DSS_URL=os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')

BUNDLE_SCHEMA_BASE_URL=os.environ.get('BUNDLE_SCHEMA_BASE_URL', 'https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/%s/json_schema/')
METADATA_SCHEMA_VERSION = os.environ.get('SCHEMA_VERSION', '4.6.1')


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
        tmpFile.write(json.dumps(self.getBundleDocument(doc),  indent=4))
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

        primarySample = self.getBundleDocument(sample)
        primarySample["derivation_protocols"] = []

        for p, protocol in enumerate(nestedProtocols):
            primarySample["derivation_protocols"].append(nestedProtocols[p]["content"])

        if nestedSamples:
            primarySample["derived_from"] = nestedSamples[0]["uuid"]["uuid"]

        return primarySample

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
            analysisBundleContent = self.getBundleDocument(analysis)
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

        # try:
        #     # self.staging_api.createStagingArea(submissionEnvelopeId)
        # except ValueError, e:
        #     self.logger.error("Can't create staging area " + str(e))


        for index, assay in enumerate(assays):

            # collect all submitted files for creation of the submission.json
            submittedFiles = []

            # create the bundle manifest to track file uuid to object uuid maps for this bundle

            bundleManifest = ingestapi.BundleManifest()
            bundleManifest.envelopeUuid = submissionEnvelopeUuid

            projectEntities = list(self.ingest_api.getRelatedEntities("projects", assay, "projects"))
            if len(projectEntities) != 1:
                raise ValueError("Can only be one project in bundle")

            project = projectEntities[0]

            # track the file names we have uploaded to staging based on uuid
            # this avoids duplicating files on staging
            projectUuid = project["uuid"]["uuid"]


            projectEntity = self.getBundleDocument(project)
            # add bundle schema reference
            projectEntity["core"] = {"type" : "project_bundle",
                                     "schema_url": self.schema_url + "project_bundle.json",
                                     "schema_version": self.schema_version}

            if projectUuid not in projectUuidToBundleData:
                projectDssUuid = str(uuid.uuid4())
                projectFileName = "project_bundle_"+str(index)+".json"

                if not self.dryrun:
                    fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, projectFileName, projectEntity, '"metadata/project"')
                    projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":fileDescription.url, "dss_uuid": projectDssUuid, "indexed": True, "content-type" : '"metadata/project"'}
                else:
                    projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":"", "dss_uuid": projectDssUuid, "indexed": True, "content-type" : '"metadata/project"'}
                    valid = self.bundle_validator.validate(projectEntity, "project")
                    if valid:
                        self.logger.info("Project entity " + projectDssUuid + " is valid")
                    else:
                        self.logger.info("Project entity " + projectDssUuid + " is not valid")
                        self.logger.info(valid)
                    self.dumpJsonToFile(projectEntity, projectEntity["content"]["project_id"],
                                        "project_bundle_" + str(index))

                bundleManifest.fileProjectMap = {projectDssUuid: [projectUuid]}

            else:
                bundleManifest.fileProjectMap = {projectUuidToBundleData[projectUuid]["dss_uuid"]: [projectUuid]}


            submittedFiles.append(projectUuidToBundleData[projectUuid])

            samples = list(self.ingest_api.getRelatedEntities("samples", assay, "samples"))
            # does this still apply? we could have more than one sample per assay, and certainly more than one sample type
            if len(samples) > 1:
                raise ValueError("Can only be one sample per assay")

            sampleBundle = {}
            sampleBundle["core"] = {"type": "sample_bundle",
                                    "schema_url": self.schema_url + "sample_bundle.json",
                                    "schema_version": self.schema_version}
            sampleBundle["samples"] = []

            sample = samples[0]

            # In v4 bundles, all samples in one derivation chain sit as equivalent objects in an array in the bundle. Starting from the assay-related sample, build up the derivation chain
            done = False
            sampleRelatedUuids = []

            assaySampleUuid = sample["uuid"]["uuid"]


            while not done:
                nestedSamples = self.getNestedObjects("derivedFromSamples", sample, "samples")
                primarySample = self.buildSampleObject(sample, nestedSamples)

                sampleBundle["samples"].append(primarySample)
                sampleUuid = sample["document_id"]
                sampleRelatedUuids.append(sampleUuid)

                if nestedSamples:
                    sample = nestedSamples[0]
                else:
                    done = True

            # if this sample derivation chain has not been seen in relation to an existing sample
            if assaySampleUuid not in sampleUuidToBundleData:
                sampleDssUuid = str(uuid.uuid4())
                sampleFileName = "sample_bundle_"+str(index)+".json"

                if not self.dryrun:
                    fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, sampleFileName, sampleBundle, '"metadata/sample"')
                    sampleUuidToBundleData[assaySampleUuid] = {"name":sampleFileName, "submittedName":"sample.json", "url":fileDescription.url, "dss_uuid": sampleDssUuid, "indexed": True, "content-type" : '"metadata/sample"'}
                else:
                    sampleUuidToBundleData[assaySampleUuid] = {"name":sampleFileName, "submittedName":"sample.json", "url":"", "dss_uuid": sampleDssUuid, "indexed": True, "content-type" : '"metadata/sample"'}
                    valid = self.bundle_validator.validate(sampleBundle, "sample")
                    if valid:
                        self.logger.info("Sample entity " + sampleDssUuid + " is valid")
                    else:
                        self.logger.info("Sample entity " + sampleDssUuid + " is not valid")
                        self.logger.info(valid)
                    self.dumpJsonToFile(sampleBundle, projectEntity["content"]["project_id"], "sample_bundle_" + str(index))

                bundleManifest.fileSampleMap = {sampleDssUuid: sampleRelatedUuids}
            # else add any new sampleUuids to the related samples list
            else:
               bundleManifest.fileSampleMap = {sampleUuidToBundleData[assaySampleUuid]["dss_uuid"]: sampleRelatedUuids}

            submittedFiles.append(sampleUuidToBundleData[assaySampleUuid])

            fileToBundleData = {}
            for file in self.ingest_api.getRelatedEntities("files", assay, "files"):
                fileUuid = file["uuid"]["uuid"]
                fileName = file["fileName"]
                cloudUrl = file["cloudUrl"]
                fileToBundleData[fileUuid] = {"name":fileName, "submittedName":fileName, "url":cloudUrl, "dss_uuid": fileUuid, "indexed": False, "content-type" : "data"}
                submittedFiles.append(fileToBundleData[fileUuid])
                bundleManifest.files.append(fileUuid)

            assayUuid = assay["uuid"]["uuid"]

            #TO DO: this is hack in v4 because the bundle schema is specified as an array rather than an object! this should be corrected in v5
            assayEntity = self.getBundleDocument(assay)
            assayEntity["core"] = {"type": "assay_bundle",
                                   "schema_url": self.schema_url + "assay_bundle.json",
                                   "schema_version": self.schema_version}

            assayEntity["has_input"] = assaySampleUuid
            assayEntity["has_output"] = fileToBundleData.keys()

            assayDssUuid = str(uuid.uuid4())
            assayFileName = "assay_bundle_" + str(index) + ".json"


            if not self.dryrun:
                fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, assayFileName, assayEntity, '"metadata/assay"')
                submittedFiles.append({"name":assayFileName, "submittedName":"assay.json", "url":fileDescription.url, "dss_uuid": assayDssUuid, "indexed": True, "content-type" : '"metadata/assay"'})
            else:
                submittedFiles.append({"name":assayFileName, "submittedName":"assay.json", "url":"", "dss_uuid": assayDssUuid, "indexed": True, "content-type" : '"metadata/assay"'})
                valid = self.bundle_validator.validate(assayEntity, "assay")
                if valid:
                    self.logger.info("Assay entity " + assayDssUuid + " is valid")
                else:
                    self.logger.info("Assay entity " + assayDssUuid + " is not valid")
                    self.logger.info(valid)

                self.dumpJsonToFile(assayEntity, projectEntity["content"]["project_id"], "assay_bundle_" + str(index))

            bundleManifest.fileAssayMap = {assayDssUuid: [assayUuid]}

            self.logger.info("All files staged...")

            # don't need protocols uuid yet, but will if protocols.json becomes a thing
            # protocolUuids = []
            # for prot in nestedProtocols:
            #     protocolUuids.append(prot["core"]["uuid"])

            if not self.dryrun:
                # write to DSS
                self.dss_api.createBundle(bundleManifest.bundleUuid, submittedFiles)
                # write bundle manifest to ingest API
                self.ingest_api.createBundleManifest(bundleManifest)

            else:
                self.dumpJsonToFile(bundleManifest.__dict__, projectEntity["content"]["project_id"], "bundleManifest_" + str(index))

            self.logger.info("bundles generated! "+bundleManifest.bundleUuid)

    def writeMetadataToStaging(self, submissionId, fileName, content, contentType):
        self.logger.info("writing to staging area..." + fileName)
        fileDescription = self.staging_api.stageFile(submissionId, fileName, content, contentType)
        self.logger.info("File staged at " + fileDescription.url)
        return fileDescription



    def getBundleDocument(self, entity):
        content = {}
        content["content"] = entity["content"]
        submissionDate = entity["submissionDate"]
        updateDate = entity["updateDate"]

        del entity["submissionDate"]
        del entity["updateDate"]
        del entity["content"]
        del entity["_links"]
        del entity["events"]
        del entity["validationState"]
        del entity["validationErrors"]

        core = entity
        content["hca_ingest"] =  core
        # need to clean the uuid from the ingest json
        uuid =  content["hca_ingest"]["uuid"]["uuid"]
        del content["hca_ingest"]["uuid"]
        content["hca_ingest"]["document_id"] = uuid
        content["hca_ingest"]["submissionDate"] = submissionDate
        content["hca_ingest"]["updateDate"] = updateDate
        if content["hca_ingest"]["accession"] is None:
            content["hca_ingest"]["accession"] = ""
        return content

    # returns a copy of a bundle manifest JSON, but with a new bundleUuid
    def makeCopyBundle(self, bundleToCopy):
        newBundle = ingestapi.BundleManifest()

        newBundle.files = bundleToCopy["files"]
        newBundle.fileSampleMap = bundleToCopy["fileSampleMap"]
        newBundle.fileAssayMap = bundleToCopy["fileAssayMap"]
        newBundle.fileProjectMap = bundleToCopy["fileProjectMap"]
        newBundle.fileProtocolMap = bundleToCopy["fileProtocolMap"]
        return newBundle

    def completeSubmission(self, submissionEnvelopeId):
        self.ingest_api.updateSubmissionState(submissionEnvelopeId, 'cleaning')
        self.ingest_api.updateSubmissionState(submissionEnvelopeId, 'complete')
        self.logger.info('Submission status is COMPLETE')

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
