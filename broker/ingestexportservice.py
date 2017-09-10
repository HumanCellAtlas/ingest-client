#!/usr/bin/env python
"""
desc goes here 
"""
__author__ = "jupp"
__license__ = "Apache 2.0"

import os
import logging
import ingestapi
import json

class IngestExporter:
    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        self.logger = logging.getLogger(__name__)
        self.ingest_api = None

    def writeManifest(self, name, index, files):
        dir = os.path.abspath("bundles/" + name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        bundleDir = os.path.abspath(dir + "/bundle" + index)
        if not os.path.exists(bundleDir):
            os.makedirs(bundleDir)

        files = {"files" : files}
        tmpFile = open(bundleDir + "/manifest.json", "w")
        tmpFile.write(json.dumps(files,  indent=4))
        tmpFile.close()

    def writeBundleToFile(self, name, index, project, sample, assay):
        dir = os.path.abspath("bundles/"+name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        bundleDir = os.path.abspath(dir+"/bundle"+index)
        if not os.path.exists(bundleDir):
            os.makedirs(bundleDir)
        tmpFile = open(bundleDir + "/project.json", "w")
        tmpFile.write(json.dumps(self.getBundleDocument(project),  indent=4))
        tmpFile.close()

        tmpFile = open(bundleDir + "/sample.json", "w")
        tmpFile.write(json.dumps(self.getBundleDocument(sample),  indent=4))
        tmpFile.close()

        tmpFile = open(bundleDir + "/assay.json", "w")
        tmpFile.write(json.dumps(self.getBundleDocument(assay),  indent=4))
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
        self.ingest_api = ingestapi.IngestApi()

        submissionUrl = self.ingest_api.getSubmissionUri(submissionId=submissionEnvelopeId)
        for index, assay in enumerate(self.ingest_api.getAssays(submissionUrl)):

            projectEntities = list(self.ingest_api.getRelatedEntities("projects", assay, "projects"))
            if len(projectEntities) > 1:
                raise ValueError("Can only be one project in bundle")

            project = projectEntities[0]

            samples = list(self.ingest_api.getRelatedEntities("samples", assay, "samples"))
            if len(samples) > 1:
                raise ValueError("Can only be one sample per assay")

            sample = samples[0]
            nestedSample = self.getNestedObjects("derivedFromSamples", sample, "samples")
            sample["content"]["donor"] = nestedSample[0]
            nestedProtocols = self.getNestedObjects("protocols", sample, "protocols")
            sample["content"]["protocols"] = nestedProtocols

            files = map(lambda f : self.getBundleDocument(f),list(self.ingest_api.getRelatedEntities("files", assay, "files")))


            # assay["content"]["files"] = files
            projectId = project["content"]["id"]
            self.writeBundleToFile(projectId, str(index), project, sample, assay)
            # write manifest file
            self.writeManifest(projectId, str(index),files)
            print "bundles generated!"

    def getBundleDocument(self, entity):
        content = entity["content"]
        del entity["content"]
        del entity["_links"]
        core = entity

        content["core"] =  core
        return content

if __name__ == '__main__':
    ex = IngestExporter()
    # ex.generateBundles("59b52e7882f53277fbd0fbc8")


