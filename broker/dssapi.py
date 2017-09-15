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
