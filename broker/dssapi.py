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
            indexed = file["indexed"]
            contentType = file["content-type"]
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
                    "indexed": indexed,
                    "name": submittedName,
                    "uuid": uuid,
                    "version": version,
                    "content-type": contentType 
                }
                bundleFile["files"].append(fileObject)
            else:
                self.logger.error('Error in creating bundle')
                self.print_response(r)
                self.print_request(r.request)
                raise ValueError('Can\'t create bundle file :' +url)

        # finally create the bundle
        bundleUrl = self.url +"/v1/bundles/"+bundleUuid
        r = requests.put(bundleUrl, data=json.dumps(bundleFile), params={"replica":"aws"}, headers=self.headers)
        if r.status_code == requests.codes.ok or r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            print ("bundle stored to dss! "+ bundleUuid)


    # analysis bundle === provenanceBundle.files (union) filesToTransfer
    #
    # provenanceBundleManifest : type dict
    # analysisBundleManifest : type IngestApi.BundleManifest
    # filesToTransfer : type List of dict() with keys "submittedName", "url" and "dss_uuid"

    def createAnalysisBundle(self, provenanceBundleManifest, analysisBundleManifest, filesToTransfer):
        provenanceBundleUuid = provenanceBundleManifest["bundleUuid"]
        analysisBundleUuid = analysisBundleManifest.bundleUuid 

        bundleCreatePayload = {"creator_uid": 8008, "files" : []}
        # transfer any new files/metadata in the secondary submission
        for fileToTransfer in filesToTransfer:
            submittedName = fileToTransfer["submittedName"]
            url = fileToTransfer["url"]
            uuid = fileToTransfer["dss_uuid"]
            indexed = fileToTransfer["indexed"]
            contentType = fileToTransfer["content-type"]

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
                    "indexed": indexed,
                    "name": submittedName,
                    "uuid": uuid,
                    "version": version,
                    "content-type" : contentType
                }
                bundleCreatePayload["files"].append(fileObject)
            else:
                self.logger.error('Error in creating analysis bundle')
                self.print_response(r)
                self.print_request(r.request)
                raise ValueError('Can\'t create bundle file :' +url)

        # merge the bundleCreatePayload.files with provenanceBundle.files
        provenanceBundleFiles = self.retrieveBundle(provenanceBundleUuid)["bundle"]["files"]
        # need to add the "indexed" key and filter out other info, else we get a 500
        bundleCreatePayload["files"] += list(map(lambda provenanceFile: {"indexed":provenanceFile["indexed"],
                                                                         "name":provenanceFile["name"],
                                                                         "uuid":provenanceFile["uuid"],
                                                                         "version":provenanceFile["version"],
                                                                         "content-type":provenanceFile["content-type"]
                                                                         },provenanceBundleFiles))

        # finally create the bundle
        bundleUrl = self.url +"/v1/bundles/"+analysisBundleUuid
        r = requests.put(bundleUrl, data=json.dumps(bundleCreatePayload), params={"replica":"aws"}, headers=self.headers)
        if r.status_code == requests.codes.ok or r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            print ("bundle stored to dss! "+ analysisBundleUuid)

    def retrieveBundle(self, bundleUuid):
        provenanceBundleUrl = self.url +"/v1/bundles/" + bundleUuid
        r = requests.get(provenanceBundleUrl, headers=self.headers, params={"replica":"aws"})
        if r.status_code == requests.codes.ok or r.status_code ==  requests.codes.created or r.status_code ==  requests.codes.accepted :
            return json.loads(r.text)
        else:
            raise ValueError("Couldn't find bundle in the DSS with uuid: " + bundleUuid)

    def print_response(self, res):
        print('RESPONSE:\n{status_code}\n{headers}\n\n{text}\n\n'.format(
            status_code=res.status_code,
            headers='\n'.join('{}: {}'.format(k, v) for k, v in res.headers.items()),
            text=res.text
        ))

    def print_request(self, req):
        print('REQUEST:\n{method} {url}\n{headers}\n\n{body}\n\n'.format(
            method=req.method,
            url=req.url,
            headers='\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
            body=req.body,
        ))