#!/usr/bin/env python
"""
desc goes here 
"""
from dssapi import DssApi
from stagingapi import StagingApi

__author__ = "jupp"
__license__ = "Apache 2.0"

import os
import logging
import ingestapi
import json
import uuid
import requests
from optparse import OptionParser
import os, sys
from stagingapi import StagingApi

DEFAULT_INGEST_URL=os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL=os.environ.get('STAGING_API', 'http://staging.dev.data.humancellatlas.org')
DEFAULT_DSS_URL=os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')

class IngestExporter:
    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        self.logger = logging.getLogger(__name__)

        parser = OptionParser()
        parser.add_option("-i", "--ingest", help="the URL to the ingest API")
        parser.add_option("-s", "--staging", help="the URL to the staging API")
        parser.add_option("-d", "--dss", help="the URL to the datastore service")
        parser.add_option("-l", "--log", help="the logging level", default='INFO')

        (options, args) = parser.parse_args()

        self.ingest_api = None

        self.ingestUrl = options.ingest if options.ingest else DEFAULT_INGEST_URL
        self.stagingUrl = options.staging if options.staging else DEFAULT_STAGING_URL
        self.dssUrl = options.dss if options.dss else DEFAULT_DSS_URL
        self.logger.debug("ingest url is "+self.ingestUrl )

        self.staging_api = StagingApi()
        self.dss_api = DssApi()

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

        # read assays from ingest API
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)

        submissionUrl = self.ingest_api.getSubmissionUri(submissionId=submissionEnvelopeId)
        submissionUuid = self.ingest_api.getObjectUuid(submissionUrl)

        # check staging area is available
        if self.staging_api.hasStagingArea(submissionUuid):
            try:
                assays = self.ingest_api.getAssays(submissionUrl)
            except:
                self.logger.info("issue getting assays for the submission, probably doesn't have any assays") # TODO

            try:
                analyses = self.ingest_api.getAnalyses(submissionUrl)
            except:
                self.logger.info("issue getting analyses for the submission, probably doesn't have any analyses") # TODO

            self.logger.info("Attempting to export primary submissions to DSS...")
            self.primarySubmission(submissionUuid,assays )

            self.logger.info("Attempting to export secondary submissions to DSS...")
            self.secondarySubmission(submissionUuid,analyses )
        else:
            self.logger.error("Can\'t do export as no staging area has been created")

    def secondarySubmission(self, submissionEnvelopeUuid, analyses):
        # list of FileDescriptors for files we need to transfer to the DSS before creating the bundle
        filesToTransfer = []

        # generate the analysis.json
        # assume there's only 1 analysis metadata, TODO:  expand later...
        for index, analysis in enumerate(analyses):

            # get the referenced bundle manififest (assume there's only 1)
            inputBundle = list(self.ingest_api.getRelatedEntities("inputBundleManifests", analysis, "bundleManifests"))[0]

            # the new bundle manifest === the old manifest (union) staged analysis file (union) new data files
            bundleManifest = makeCopyBundle(inputBundle)

            # add the referenced files to the bundle manifest and to the files to transfer
            files = list(self.ingest_api.getRelatedEntities("files", analysis, "files"))
            bundleManifest.files += list(map(lambda file_json : file_json["uuid"]["uuid"], files))
            filesToTransfer += list(map(lambda file_json: {"name": file_json["fileName"],
                                                           "submittedName": file_json["fileName"], 
                                                           "url": file_json["cloudUrl"], 
                                                           "dss_uuid": file_json["uuid"]["uuid"]
                                                           }, files))

            # stage the analysis.json, add to filesToTransfer and to the bundle manifest
            analysisDssUuid = unicode(uuid.uuid4())
            analysisBundleContent = getBundleContent(analysis)
            analysisFileName = "analysis_0.json" # TODO: shouldn't be hardcoded
            fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, analysisFileName, analysisBundleContent, "hca-analysis")
            
            bundleManifest.fileAnalysisMap[analysisDssUuid] = [analysis["uuid"]["uuid"]]
            filesToTransfer.append({"name":analysisFileName, "submittedName":"analysis.json", "url":fileDescription.url, "dss_uuid": analysisDssUuid})

            # generate new bundle
            # write to DSS
            self.dss_api.createAnalysisBundle(inputBundleUuid, bundleManifest, filesToTransfer)

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

            projectEntities = list(self.ingest_api.getRelatedEntities("projects", assay, "projects"))
            if len(projectEntities) != 1:
                raise ValueError("Can only be one project in bundle")

            project = projectEntities[0]

            # track the file names we have uploaded to staging based on uuid
            # this avoids duplicating files on staging
            projectUuid = project["uuid"]["uuid"]
            projectBundle = self.getBundleDocument(project)

            if projectUuid not in projectUuidToBundleData:
                projectDssUuid = unicode(uuid.uuid4())
                projectFileName = "project_"+str(index)+".json"
                fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, projectFileName, projectBundle, "hca-project")
                projectUuidToBundleData[projectUuid] = {"name":projectFileName,"submittedName":"project.json", "url":fileDescription.url, "dss_uuid": projectDssUuid}

                bundleManifest.fileProjectMap = {projectDssUuid: [projectUuid]}
            else:
                bundleManifest.fileProjectMap = {projectUuidToBundleData[projectUuid]["dss_uuid"]: [projectUuid]}
            submittedFiles.append(projectUuidToBundleData[projectUuid])

            samples = list(self.ingest_api.getRelatedEntities("samples", assay, "samples"))
            if len(samples) > 1:
                raise ValueError("Can only be one sample per assay")

            sample = samples[0]
            nestedSample = self.getNestedObjects("derivedFromSamples", sample, "samples")
            sample["content"]["donor"] = nestedSample[0]
            nestedProtocols = self.getNestedObjects("protocols", sample, "protocols")
            sample["content"]["protocols"] = nestedProtocols
            sampleUuid = sample["uuid"]["uuid"]
            sampleRelatedUuids = [sampleUuid, sample["content"]["donor"]["core"]["uuid"]]

            sampleBundle = self.getBundleDocument(sample)

            if sampleUuid not in sampleUuidToBundleData:
                sampleDssUuid = unicode(uuid.uuid4())
                sampleFileName = "sample_"+str(index)+".json"
                fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, sampleFileName, sampleBundle, "hca-sample")
                sampleUuidToBundleData[sampleUuid] = {"name":sampleFileName, "submittedName":"sample.json", "url":fileDescription.url, "dss_uuid": sampleDssUuid}
                bundleManifest.fileSampleMap = {sampleDssUuid: sampleRelatedUuids}
            else:
                bundleManifest.fileSampleMap = {sampleUuidToBundleData[sampleUuid]["dss_uuid"]: sampleRelatedUuids}

            submittedFiles.append(sampleUuidToBundleData[sampleUuid])

            fileToBundleData = {}
            for file in self.ingest_api.getRelatedEntities("files", assay, "files"):
                fileUuid = file["uuid"]["uuid"]
                fileName = file["fileName"]
                cloudUrl = file["cloudUrl"]
                fileToBundleData[fileUuid] = {"name":fileName, "submittedName":fileName, "url":cloudUrl, "dss_uuid": fileUuid}
                submittedFiles.append(fileToBundleData[fileUuid])
                bundleManifest.files.append(fileUuid)

            assayUuid = assay["uuid"]["uuid"]
            assaysBundle = self.getBundleDocument(assay)
            assayDssUuid = unicode(uuid.uuid4())
            assayFileName = "assay_" + str(index) + ".json"

            fileDescription = self.writeMetadataToStaging(submissionEnvelopeUuid, assayFileName, assaysBundle, "hca-assay")
            bundleManifest.fileAssayMap = {assayDssUuid: [assayUuid]}
            submittedFiles.append({"name":assayFileName, "submittedName":"assay.json", "url":fileDescription.url, "dss_uuid": assayDssUuid})

            self.logger.info("All files staged...")

            # don't need protocols uuid yet, but will if protocols.json becomes a thing
            # protocolUuids = []
            # for prot in nestedProtocols:
            #     protocolUuids.append(prot["core"]["uuid"])

            # write to DSS

            self.dss_api.createBundle(bundleManifest.bundleUuid, submittedFiles)

            # write bundle manifest to ingest API

            self.ingest_api.createBundleManifest(bundleManifest)
            self.logger.info("bundles generated! "+bundleManifest.bundleUuid)

    def writeMetadataToStaging(self, submissionId, fileName, content, contentType):
        self.logger.info("writing to staging area..." + fileName)
        fileDescription = self.staging_api.stageFile(submissionId, fileName, content, contentType)
        self.logger.info("File staged at " + fileDescription.url)
        return fileDescription



    def getBundleDocument(self, entity):
        content = entity["content"]
        del entity["content"]
        del entity["_links"]
        core = entity
        content["core"] =  core
        # need to clean the uuid from the ingest json
        uuid =  content["core"]["uuid"]["uuid"]
        del content["core"]["uuid"]
        content["core"]["uuid"] = uuid
        return content

    # returns a copy of a bundle manifest JSON, but with a new bundleUuid
    def makeCopyBundle(self, bundleToCopy):
        newBundle = ingestapi.BundleManifest

        newBundle.files = bundleToCopy["files"]
        newBundle.fileSampleMap = bundleToCopy["fileSampleMap"] 
        newBundle.fileAssayMap = bundleToCopy["fileAssayMap"]
        newBundle.fileProjectMap = bundleToCopy["fileProjectMap"]
        newBundle.fileProtocolMap = bundleToCopy["fileProtocolMap"]
        return newBundle

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

    ex = IngestExporter()
