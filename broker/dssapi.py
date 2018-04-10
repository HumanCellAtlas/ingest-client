#!/usr/bin/env python
"""
Description goes here
"""
import datetime

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"

#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"

import json
import logging
import os
import requests
import time

import glob
import urllib
import hca

class DssApi:
    def __init__(self, url=None):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)
        self.url = url if url else "http://dss.dev.data.humancellatlas.org"
        if not url and 'DSS_API' in os.environ:
            url = os.environ['DSS_API']
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info("using " +url+ " for dss API")

        self.headers = {'Content-type': 'application/json'}


    def createBundle(self, bundleUuid, submittedFiles):

        hca_client = hca.dss.DSSClient()
        hca_client.host = self.url + "/v1"

        bundleFile = {"creator_uid": 8008, "files" : []}
        for file in submittedFiles:
            submittedName = file["submittedName"]
            url = file["url"]
            uuid = file["dss_uuid"]
            indexed = file["indexed"]
            contentType = file["content-type"]
            if not url:
                self.logger.warn("can't create bundle for "+submittedName+" as no cloud URL is provided")
                continue

            try:
                file_submission_data = hca_client.put_file(
                    uuid=uuid,
                    bundle_uuid=bundleUuid,
                    creator_uid=bundleFile["creator_uid"],
                    source_url=url
                )
                self.logger.debug("Bundle file submited "+url)
            except Exception as e:
                self.logger.error('Error in creating bundle')
                raise ValueError('Can\'t create bundle file :' +url)

            version = file_submission_data["version"]

            fileObject = {
                "indexed": indexed,
                "name": submittedName,
                "uuid": uuid,
                "version": version,
                "content-type": contentType 
            }
            bundleFile["files"].append(fileObject)

        # Generate version client-side for idempotent PUT /bundle
        version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        # finally create the bundle
        try:
            hca_client.put_bundle(
                uuid=bundleUuid,
                version=version,
                replica="aws",
                files=bundleFile["files"],
                creator_uid=bundleFile["creator_uid"]
            )
            print ("bundle stored to dss! " + bundleUuid)
        except Exception as e:
            self.logger.error('Error in creating analysis bundle')
            raise ValueError('Can\'t create bundle:' + bundleUuid)

    def _put_bundle_file(self, fileUrl, requestBody):
        r = requests.put(fileUrl, data=json.dumps(requestBody), headers=self.headers)
        return r

    def _put_bundle(self, bundleUrl, bundleFile):
        r = requests.put(bundleUrl, data=json.dumps(bundleFile), params={"replica":"aws"}, headers=self.headers)
        return r

    # analysis bundle === provenanceBundle.files (union) filesToTransfer
    #
    # provenanceBundleManifest : type dict
    # analysisBundleManifest : type IngestApi.BundleManifest
    # filesToTransfer : type List of dict() with keys "submittedName", "url" and "dss_uuid"

    def createAnalysisBundle(self, provenanceBundleManifest, analysisBundleManifest, filesToTransfer):
        provenanceBundleUuid = provenanceBundleManifest["bundleUuid"]
        analysisBundleUuid = analysisBundleManifest.bundleUuid 

        hca_client = hca.dss.DSSClient()
        hca_client.host = self.url + "/v1"

        bundleCreatePayload = {"creator_uid": 8008, "files" : []}
        # transfer any new files/metadata in the secondary submission
        for fileToTransfer in filesToTransfer:
            submittedName = fileToTransfer["submittedName"]
            url = fileToTransfer["url"]
            uuid = fileToTransfer["dss_uuid"]
            indexed = fileToTransfer["indexed"]
            contentType = fileToTransfer["content-type"]

            try:
                file_submission_data = hca_client.put_file(
                    uuid=uuid,
                    bundle_uuid=analysisBundleUuid,
                    creator_uid=bundleCreatePayload["creator_uid"],
                    source_url=url
                )
                self.logger.debug("Bundle file submited "+url)
            except Exception as e:
                self.logger.error('Error in creating analysis bundle')
                raise ValueError('Can\'t create bundle file :' +url)

            version = file_submission_data["version"]

            fileObject = {
                "indexed": indexed,
                "name": submittedName,
                "uuid": uuid,
                "version": version,
                "content-type" : contentType
            }

            bundleCreatePayload["files"].append(fileObject)

        # merge the bundleCreatePayload.files with provenanceBundle.files
        provenanceBundleFiles = self.retrieveBundle(provenanceBundleUuid)["bundle"]["files"]
        # need to add the "indexed" key and filter out other info, else we get a 500
        bundleCreatePayload["files"] += list(map(lambda provenanceFile: {"indexed":provenanceFile["indexed"],
                                                                         "name":provenanceFile["name"],
                                                                         "uuid":provenanceFile["uuid"],
                                                                         "version":provenanceFile["version"],
                                                                         "content-type":provenanceFile["content-type"]
                                                                         },provenanceBundleFiles))

        # Generate version client-side for idempotent PUT /bundle
        version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        # finally create the bundle
        try:
            hca_client.put_bundle(
                uuid=analysisBundleUuid,
                version=version,
                replica="aws",
                files=bundleCreatePayload["files"],
                creator_uid=bundleCreatePayload["creator_uid"]
            )
            print ("bundle stored to dss! "+ analysisBundleUuid)
        except Exception as e:
            self.logger.error('Error in creating analysis bundle')
            raise ValueError('Can\'t create bundle:' + analysisBundleUuid)

    def retrieveBundle(self, bundleUuid):
        provenanceBundleUrl = self.url +"/v1/bundles/" + bundleUuid
        r = requests.get(provenanceBundleUrl, headers=self.headers, params={"replica":"aws"})
        if r.status_code == requests.codes.ok or r.status_code ==  requests.codes.created or r.status_code ==  requests.codes.accepted :
            return json.loads(r.text)
        else:
            raise ValueError("Couldn't find bundle in the DSS with uuid: " + bundleUuid)
