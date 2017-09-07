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
        logging.basicConfig(formatter=formatter,level=logging.INFO)
        self.ingest_api = None

    def writeBundleToFile(self, name, index, project, sample, assay):
        dir = os.path.abspath("bundles/"+name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        bundleDir = os.path.abspath(dir+"/bundle"+index)
        if not os.path.exists(bundleDir):
            os.makedirs(bundleDir)
        tmpFile = open(bundleDir + "/project.json", "w")
        tmpFile.write(json.dumps(self.getBundleDocument(project)))
        tmpFile.close()

        tmpFile = open(bundleDir + "/sample.json", "w")
        tmpFile.write(json.dumps(self.getBundleDocument(sample)))
        tmpFile.close()

        tmpFile = open(bundleDir + "/assay.json", "w")
        tmpFile.write(json.dumps(self.getBundleDocument(assay)))
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
        logging.info('submission received '+submissionEnvelopeId)
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
            nested = self.getNestedObjects("derivedFromSamples", sample, "samples")
            sample["content"]["derivedFromSamples"] = nested

            files = map(lambda f : self.getBundleDocument(f),list(self.ingest_api.getRelatedEntities("files", assay, "files")))

            assay["content"]["files"] = files

            self.writeBundleToFile(project["content"]["id"], str(index), project, sample, assay)

    def getBundleDocument(self, entity):
        content = entity["content"]
        del entity["content"]
        del entity["_links"]
        core = entity

        bundleDocument = \
            {"core": core,
             "content": content}
        return bundleDocument

if __name__ == '__main__':
    ex = IngestExporter()
    ex.generateBundles("59b19f0782f5329d05938b8d")

