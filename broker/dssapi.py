#!/usr/bin/env python
"""
Description goes here
"""
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

import glob, json, os, urllib, requests, logging
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

    def createBundle(self,bundleUuid, submittedFiles):

        bundleFile = {"creator_uid": 8008, "files" : []}
        for file in submittedFiles:
            submittedName = file["submittedName"]
            url = file["url"]
            uuid = file["dss_uuid"]

            if not url:
                self.logger.warn("can't create bundle for "+submittedName+" as no cloud URL is provided")
                continue
            requestBody = {
                          "bundle_uuid": bundleUuid,
                          "creator_uid": 8008,
                          "source_url": url
                        }
            fileUrl = self.url +"/v1/files/"+uuid
            r = requests.put(fileUrl, data=json.dumps(requestBody), headers=self.headers)
            if r.status_code == requests.codes.ok or r.status_code ==  requests.codes.created or r.status_code ==  requests.codes.accepted :
                self.logger.debug("Bundle file submited "+url)
                version = json.loads(r.text)["version"]
                fileObject = {
                    "indexed": True,
                    "name": submittedName,
                    "uuid": uuid,
                    "version": version
                }
                bundleFile["files"].append(fileObject)
            else:
                raise ValueError('Can\'t create bundle file :' +url)

        # finally create the bundle
        bundleUrl = self.url +"/v1/bundles/"+bundleUuid
        r = requests.put(bundleUrl, data=json.dumps(bundleFile), params={"replica":"aws"}, headers=self.headers)
        if r.status_code == requests.codes.ok or r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            print "bundle stored to dss! "+ bundleUuid


    # analysis bundle === provenanceBundle.files (union) filesToTransfer
    #
    # provenanceBundleManifest : type dict
    # analysisBundleManifest : type IngestApi.BundleManifest
    # submittedFiles : type List of FileDescriptor
    def createAnalysisBundle(self, provenanceBundleManifest, analysisBundleManifest, filesToTransfer):
        provenanceBundleUuid = provenanceBundleManifest["bundleUuid"] # type: dict
        analysisBundleUuid = analysisBundleManifest.bundleUuid # type BundleManifest

        bundleCreatePayload = {"creator_uid": 8008, "files" : []}
        for fileToTransfer in filesToTransfer:
            submittedName = file["submittedName"]
            url = file["url"]
            uuid = file["dss_uuid"]

            requestBody = {
                          "bundle_uuid": analysisBundleUuid, # TODO: referring to bundle before it's created might be dodgy?
                          "creator_uid": 8008,
                          "source_url": url
                        }
 
            fileUrl = self.url +"/v1/files/"+uuid

            r = requests.put(fileUrl, data=json.dumps(requestBody), headers=self.headers)
            if r.status_code == requests.codes.ok or r.status_code ==  requests.codes.created or r.status_code ==  requests.codes.accepted :
                self.logger.debug("Bundle file submited "+url)
                version = json.loads(r.text)["version"]
                fileObject = {
                    "indexed": True,
                    "name": submittedName,
                    "uuid": uuid,
                    "version": version
                }
                bundleCreatePayload["files"].append(fileObject)
            else:
                raise ValueError('Can\'t create bundle file :' +url)

        # merge the bundleCreatePayload.files with provenanceBundle.files
        provenanceBundleFiles = retrieveBundle(provenanceBundleUuid)["files"]
        bundleCreatePayload["files"] += provenanceBundleFiles

        # finally create the bundle
        bundleUrl = self.url +"/v1/bundles/"+analysisBundleUuid
        r = requests.put(bundleUrl, data=json.dumps(bundleCreatePayload), params={"replica":"aws"}, headers=self.headers)
        if r.status_code == requests.codes.ok or r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            print "bundle stored to dss! "+ analysisBundleUuid

    def retrieveBundle(self, bundleUuid):
        provenanceBundleUrl = self.url +"/v1/bundles/" + bundleUuid
        r = requests.get(provenanceBundleUrl, headers=self.headers)
        if r.status_code == requests.codes.ok or r.status_code ==  requests.codes.created or r.status_code ==  requests.codes.accepted :
            return json.loads(r.text)

