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
import bundlevalidator

DEFAULT_INGEST_URL=os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL=os.environ.get('STAGING_API', 'http://staging.dev.data.humancellatlas.org')
DEFAULT_DSS_URL=os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')



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
        self.logger.debug("ingest url is "+self.ingestUrl)

        self.staging_api = StagingApi()
        self.dss_api = DssApi()
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)

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
            children = self.getNestedObjects(relation, nested, entityType)
            if len(children) > 0:
                nested["content"][relation] = children
            nestedChildren.append(self.getBundleDocument(nested))
        return nestedChildren

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
                                                           "indexed" : False
                                                           }, files))

            # stage the analysis.json, add to filesToTransfer and to the bundle manifest
            analysisUuid = analysis["uuid"]["uuid"]
            analysisDssUuid = unicode(uuid.uuid4())
            analysisBundleContent = self.getBundleDocument(analysis)
            analysisFileName = "analysis_0.json" # TODO: shouldn't be hardcoded

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


            #TO DO: this is hack in v4 because the bundle schema is specified as an array rather than an object! this should be corrected in v5
            projectBundle = []
            projectEntity = self.getBundleDocument(project)
            projectBundle.append(projectEntity)

            if projectUuid not in projectUuidToBundleData:
                projectDssUuid = str(uuid.uuid4())
                projectFileName = "project_bundle_"+str(index)+".json"

                if not self.dryrun:
                    fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, projectFileName, projectBundle, '"metadata/project"')
                    projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":fileDescription.url, "dss_uuid": projectDssUuid, "indexed": True, "content-type" : '"metadata/project"'}
                else:
                    projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":"", "dss_uuid": projectDssUuid, "indexed": True, "content-type" : '"metadata/project"'}
                    bundlevalidator.validate(projectBundle, "project")
                    self.dumpJsonToFile(projectBundle, projectBundle[0]["content"]["project_id"],
                                        "project_bundle_" + str(index))

                bundleManifest.fileProjectMap = {projectDssUuid: [projectUuid]}

            else:
                bundleManifest.fileProjectMap = {projectUuidToBundleData[projectUuid]["dss_uuid"]: [projectUuid]}


            submittedFiles.append(projectUuidToBundleData[projectUuid])

            samples = list(self.ingest_api.getRelatedEntities("samples", assay, "samples"))
            # does this still apply? we could have more than one sample per assay, and certainly more than one sample type
            if len(samples) > 1:
                raise ValueError("Can only be one sample per assay")

            sample = samples[0]
            nestedSample = self.getNestedObjects("derivedFromSamples", sample, "samples")
            nestedProtocols = self.getNestedObjects("protocols", sample, "protocols")

            sampleBundle = []
            primarySample = self.getBundleDocument(sample)
            primarySample["derivation_protocols"] = []

            for p, protocol in enumerate(nestedProtocols):
                primarySample["derivation_protocols"].append(nestedProtocols[p]["content"])

            primarySample["derived_from"] = nestedSample[0]["hca_ingest"]["document_id"]

            sampleBundle.append(primarySample)
            sampleBundle.append(nestedSample[0])
            sampleUuid = sample["document_id"]
            sampleRelatedUuids = [sampleUuid, sampleBundle[1]["hca_ingest"]["document_id"]]


            if sampleUuid not in sampleUuidToBundleData:
                sampleDssUuid = str(uuid.uuid4())
                sampleFileName = "sample_bundle_"+str(index)+".json"

                if not self.dryrun:
                    fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, sampleFileName, sampleBundle, '"metadata/sample"')
                    sampleUuidToBundleData[sampleUuid] = {"name":sampleFileName, "submittedName":"sample.json", "url":fileDescription.url, "dss_uuid": sampleDssUuid, "indexed": True, "content-type" : '"metadata/sample"'}

                else:
                    sampleUuidToBundleData[sampleUuid] = {"name":sampleFileName, "submittedName":"sample.json", "url":"", "dss_uuid": sampleDssUuid, "indexed": True, "content-type" : '"metadata/sample"'}
                    bundlevalidator.validate(sampleBundle, "sample")
                    self.dumpJsonToFile(sampleBundle, projectBundle[0]["content"]["project_id"], "sample_bundle_" + str(index))

                bundleManifest.fileSampleMap = {sampleDssUuid: sampleRelatedUuids}
            else:
                bundleManifest.fileSampleMap = {sampleUuidToBundleData[sampleUuid]["dss_uuid"]: sampleRelatedUuids}

            submittedFiles.append(sampleUuidToBundleData[sampleUuid])

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
            assaysBundle = []
            assayEntity = self.getBundleDocument(assay)
            assaysBundle.append(assayEntity)


            assayDssUuid = str(uuid.uuid4())
            assayFileName = "assay_bundle_" + str(index) + ".json"


            if not self.dryrun:
                fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, assayFileName, assaysBundle, '"metadata/assay"')
                submittedFiles.append({"name":assayFileName, "submittedName":"assay.json", "url":fileDescription.url, "dss_uuid": assayDssUuid, "indexed": True, "content-type" : '"metadata/assay"'})
            else:
                submittedFiles.append({"name":assayFileName, "submittedName":"assay.json", "url":"", "dss_uuid": assayDssUuid, "indexed": True, "content-type" : '"metadata/assay"'})
                bundlevalidator.validate(assaysBundle, "assay")
                self.dumpJsonToFile(assaysBundle, projectBundle[0]["content"]["project_id"], "assay_bundle_" + str(index))

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
                self.dumpJsonToFile(bundleManifest.__dict__, projectBundle[0]["content"]["project_id"], "bundleManifest_" + str(index))

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

    (options, args) = parser.parse_args()
    if not options.submissionsEnvelopeId:
        print ("You must supply a submission envelope ID")
        exit(2)

    ex = IngestExporter(options)
    ex.generateBundles(options.submissionsEnvelopeId)
