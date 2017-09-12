#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"

import glob, json, os, urllib, requests, logging
from time import sleep
class StagingApi:
    def __init__(self, url=None):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)
        self.url = url if url else "http://staging.dev.data.humancellatlas.org"
        if not url and 'STAGING_API' in os.environ:
            url = os.environ['STAGING_API']
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info("using " +url+ " for staging API")

    def createStagingArea(self,submissionId):
        r = requests.post(self.url+"/area")
        if r.status_code == requests.codes.created:
            print "Waiting 10 seconds for IAM policy to take effect...",
            sleep(10)
            print "done"
            return True

        raise ValueError('Can\'t create staging area for sub id:' +submissionId)

    def deleteStagingArea(self,submissionId):
        r = requests.delete(self.url+"/area")
        if r.status_code == requests.codes.no_content:
            return True

        raise ValueError('Can\'t create staging area for sub id:' +submissionId)

    def stageFile(self,submissionId, filename, body, type):

        fileUrl= self.url+"/area/"+submissionId+"/"+filename

        r = requests.put(fileUrl,  data=json.dumps(body))
        if r.status_code == requests.codes.ok or requests.codes.created:
            responseObject = json.loads(r.text)
            return FileDescription(responseObject["checksums"],type,responseObject["name"],responseObject["size"],responseObject["url"], )
        raise ValueError('Can\'t create staging area for sub id:' +submissionId)

    def hasStagingArea(self, submissionId):
        fileUrl = self.url + "/area/" + submissionId
        r = requests.get(fileUrl)
        return r.status_code == requests.codes.ok


class FileDescription:
    def __init__(self, checksums, contentType, name, size, url):
        self.checksums = checksums
        self.content_type = contentType
        self.name = name
        self.size = size
        self.url = url
