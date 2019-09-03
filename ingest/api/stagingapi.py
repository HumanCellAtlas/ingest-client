#!/usr/bin/env python
"""
Description goes here
"""
from requests import HTTPError

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"

import json
import logging
import os
from time import time
from urllib.parse import urljoin

import requests
import requests.packages.urllib3.util.retry as retry

DEFAULT_STAGING_URL = os.environ.get('STAGING_API', 'https://upload.dev.data.humancellatlas.org')
DEFAULT_STAGING_VERSION = os.environ.get('STAGING_API_VERSION', 'v1')
INGEST_API_KEY = os.environ.get('INGEST_API_KEY', 'zero-pupil-until-funny')


class RetryPolicy(retry.Retry):
    def __init__(self, retry_after_status_codes={301}, **kwargs):
        super(RetryPolicy, self).__init__(**kwargs)
        self.RETRY_AFTER_STATUS_CODES = frozenset(retry_after_status_codes | retry.Retry.RETRY_AFTER_STATUS_CODES)


class UploadResponseParseException(Exception):
    pass


class FileDescription:

    def __init__(self, checksums: dict, content_type: str, name: str, size, url: str):
        self.checksums = checksums
        self.content_type = content_type
        self.name = name
        self.size = size
        self.url = url

    @staticmethod
    def parse_upload_response(res: dict):
        try:
            return FileDescription(res['checksums'], res['content_type'], res['name'], res['size'], res['url'])
        except (KeyError, TypeError) as e:
            raise UploadResponseParseException() from e


class MetadataFileStagingRequest:
    def __init__(self, staging_area_uuid, filename, metadata_json, metadata_type):
        self.staging_area_uuid = staging_area_uuid
        self.filename = filename
        self.metadata_json = metadata_json
        self.metadata_type = metadata_type


class FileUploadFailed(Exception):
    pass


class StagingApi:
    def __init__(self, url=None, apikey=None, apiversion=None):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)

        retry_policy = RetryPolicy(
            total=100,  # seems that this has a default value of 10,
            # setting this to a very high number so that it'll respect the status retry count
            status=17,  # status is the no. of retries if response is in status_forcelist,
            # this count will retry for ~20mins with back off timeout within
            read=10,
            status_forcelist=[500, 502, 503, 504],
            backoff_factor=0.6,
            method_whitelist=frozenset(['HEAD', 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'TRACE'])
        )

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_policy)
        self.session.mount('https://', adapter)

        self.logger = logging.getLogger(__name__)

        if not apiversion and DEFAULT_STAGING_VERSION:
            self.apiversion = DEFAULT_STAGING_VERSION
        self.apiversion = apiversion if apiversion else "v1"

        if not url and DEFAULT_STAGING_URL:
            url = DEFAULT_STAGING_URL
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info(f'Using {url} for staging API')
        self.url = url if url else 'https://upload.dev.data.humancellatlas.org'

        if not apikey and INGEST_API_KEY:
            apikey = INGEST_API_KEY
        self.apikey = apikey if apikey else 'zero-pupil-until-funny'

        self.header = {'Api-Key': self.apikey, 'Content-type': 'application/json'}

    def createStagingArea(self, submissionId):
        start_time = time()
        self.logger.info('Creating staging area!')
        base = urljoin(self.url, self.apiversion + '/area/' + submissionId)

        r = self.session.post(base, headers=self.header)
        r.raise_for_status()
        self.logger.info(f'Staging area created!: {base}')
        self.logger.info("Execution Time: %s seconds" % (time() - start_time))
        return r.json()

    def deleteStagingArea(self, submissionId):
        self.logger.info('Deleting staging area!')
        base = urljoin(self.url, self.apiversion + '/area/' + submissionId)
        r = self.session.delete(base, headers=self.header)
        r.raise_for_status()
        self.logger.info('Staging area deleted!')
        return base

    def stageFileRequest(self, file_stage_request: 'MetadataFileStagingRequest'):
        return self.stageFile(file_stage_request.staging_area_uuid,
                              file_stage_request.filename,
                              file_stage_request.metadata_json,
                              file_stage_request.metadata_type)

    def stageFile(self, staging_area_uuid, file_name, body, type):
        file_url = urljoin(self.url, self.apiversion + '/area/' + staging_area_uuid + "/" + file_name)

        self.logger.info(f'Staging file: {file_url}')

        header = dict(self.header)
        header['Content-type'] = f'application/json; dcp-type="{type}"'

        r = self.session.put(file_url, data=json.dumps(body, indent=4), headers=header)

        try:
            r.raise_for_status()
            res = r.json()
            return FileDescription.parse_upload_response(res)
        except (HTTPError, UploadResponseParseException) as exception:
            raise FileUploadFailed(f'File upload failed for {file_url}.') from exception

    def hasStagingArea(self, submissionId) -> bool:
        base = urljoin(self.url, self.apiversion + '/area/' + submissionId)
        r = self.session.head(base, headers=self.header)
        return r.status_code == requests.codes.ok
