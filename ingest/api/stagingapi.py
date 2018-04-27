#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"

import json
import os
import requests
import logging

from time import sleep
from urllib.parse import urljoin

DEFAULT_STAGING_URL = os.environ.get('STAGING_API', 'https://staging.dev.data.humancellatlas.org')
DEFAULT_STAGING_VERSION = os.environ.get('STAGING_API_VERSION', 'v1')
INGEST_API_KEY = os.environ.get('INGEST_API_KEY', 'zero-pupil-until-funny')


class StagingApi:
    def __init__(self, url=None, apikey=None, apiversion=None):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)

        if not apiversion and DEFAULT_STAGING_VERSION:
            self.apiversion = DEFAULT_STAGING_VERSION
        self.apiversion = apiversion if apiversion else "v1"

        if not url and DEFAULT_STAGING_URL:
            url = DEFAULT_STAGING_URL
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info("using " + url + " for staging API")
        self.url = url if url else "https://staging.dev.data.humancellatlas.org"

        if not apikey and INGEST_API_KEY:
            apikey = INGEST_API_KEY
        self.apikey = apikey if apikey else "zero-pupil-until-funny"

        self.header = {'Api-Key': self.apikey, 'Content-type': 'application/json'}

    def createStagingArea(self, submissionId):
        base = urljoin(self.url, self.apiversion + '/area/' + submissionId)
        r = requests.post(base, headers=self.header)
        if r.status_code == requests.codes.created:
            print ("Waiting 10 seconds for IAM policy to take effect..."),
            sleep(10)
            print ("staging area created!:" + base)
            return json.loads(r.text)

        raise ValueError('Can\'t create staging area for sub id:' + submissionId + ', Error:' + r.text)

    def deleteStagingArea(self, submissionId):
        base = urljoin(self.url, self.apiversion + '/area/' + submissionId)
        try:

            r = requests.delete(base, headers=self.header)
            if r.status_code == requests.codes.no_content:
                print ("staging area deleted!")
                return base
            else:
                return base
        except:
            raise ValueError('Can\'t delete staging area for sub id:' + submissionId)

    def stageFile(self, submissionId, filename, body, type):

        fileUrl = urljoin(self.url, self.apiversion + '/area/' + submissionId + "/" + filename)

        header = dict(self.header)
        header['Content-type'] = 'application/json; dcp-type=' + type

        r = self._retry_when_http_error(0, self._put_file, fileUrl, body, header)

        if r and (r.status_code == requests.codes.ok or requests.codes.created):
            responseObject = json.loads(r.text)
            return FileDescription(responseObject["checksums"], type, responseObject["name"], responseObject["size"],
                                   responseObject["url"], )

        raise ValueError('Can\'t create staging area for sub id:' + submissionId)

    def _put_file(self, fileUrl, body, header):
        r = requests.put(fileUrl, data=json.dumps(body, indent=4), headers=header)
        return r

    def hasStagingArea(self, submissionId):
        base = urljoin(self.url, self.apiversion + '/area/' + submissionId + '/files_info')

        r = requests.put(base, data="{}", headers=self.header)
        return r.status_code == requests.codes.ok

    """
        func should return http response r
    """

    def _retry_when_http_error(self, tries, func, *args):
        max_retries = 5

        if tries < max_retries:
            self.logger.debug("no of tries: " + str(tries + 1))
            r = func(*args)

            try:
                r.raise_for_status()
            except requests.HTTPError:
                self.logger.error("\nResponse was: " + str(r.status_code) + " (" + r.text + ")")
                tries += 1
                sleep(1)
                r = self._retry_when_http_error(tries, func, *args)

            return r
        else:
            error_message = "Maximum no of tries reached: " + str(max_retries)
            self.logger.error(error_message)
            return None


class FileDescription:
    def __init__(self, checksums, contentType, name, size, url):
        self.checksums = checksums
        self.content_type = contentType
        self.name = name
        self.size = size
        self.url = url
