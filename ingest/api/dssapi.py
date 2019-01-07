#!/usr/bin/env python
"""
Description goes here
"""
import datetime
import hca
import json
import logging
import os
import time
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"

AUTH_INFO_ENV_VAR = "EXPORTER_AUTH_INFO"


class DssApi:
    def __init__(self, url=None):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.url = url if url else "https://dss.dev.data.humancellatlas.org"
        if not url and 'DSS_API' in os.environ:
            url = os.environ['DSS_API']
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info("using " + url + " for dss API")

        self.headers = {'Content-type': 'application/json'}

        self.hca_client = hca.dss.DSSClient(swagger_url=f'{self.url}/v1/swagger.json')
        self.hca_client.host = self.url + "/v1"
        self.creator_uid = 8008

    def put_file(self, bundle_uuid, file):
        url = file["url"]
        uuid = file["dss_uuid"]

        version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        update_date = file.get("update_date")

        if update_date:
            update_date = datetime.datetime.strptime(update_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            update_date = update_date.strftime("%Y-%m-%dT%H%M%S.%fZ")
            version = update_date

        # retrying file creation 20 times
        max_retries = 20
        tries = 0
        file_create_complete = False

        params = {
            'uuid': uuid,
            'version': version,
            'bundle_uuid': bundle_uuid,
            'creator_uid': self.creator_uid,
            'source_url': url
        }

        while not file_create_complete and tries < max_retries:
            try:
                tries += 1
                self.logger.info(f'Creating file {file["name"]} in DSS {uuid}:{version} with params: {json.dumps(params)}')
                bundle_file = self.hca_client.put_file(
                    uuid=uuid,
                    version=version,
                    bundle_uuid=bundle_uuid,
                    creator_uid=self.creator_uid,
                    source_url=url
                )
                self.logger.info('Created!')
                file_create_complete = True
                return bundle_file
            except Exception as e:
                self.logger.error(
                    'Attempt {0} out of {1}: Error in hca_client.put_file method call with params:{2} due to {3}'.format(
                        str(tries),
                        str(max_retries),
                        json.dumps(params),
                        str(e))
                )

                if not tries < max_retries:
                    raise Error(e)
                else:
                    time.sleep(60)

    def put_bundle(self, bundle_uuid, bundle_files):
        bundle = None

        # Generate version client-side for idempotent PUT /bundle
        version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        # retrying file creation 20 times
        max_retries = 20
        tries = 0
        bundle_create_complete = False

        # finally create the bundle
        while not bundle_create_complete and tries < max_retries:
            try:
                tries += 1
                self.logger.info(f'Creating bundle in DSS {bundle_uuid}:{version}')
                bundle = self.hca_client.put_bundle(
                    uuid=bundle_uuid,
                    version=version,
                    replica="aws",
                    files=bundle_files,
                    creator_uid=self.creator_uid
                )
                self.logger.info('Created!')
                bundle_create_complete = True
                return bundle
            except Exception as e:
                params = {
                    'uuid': bundle_uuid,
                    'version': version,
                    'replica': "aws",
                    'files': bundle_files,
                    'creator_uid': self.creator_uid
                }
                self.logger.error(
                    'Attempt {0} out of {1}: Error in hca_client.put_bundle method call with params:{2} due to {3}'.format(
                        str(tries),
                        str(max_retries),
                        json.dumps(params),
                        str(e))
                )
                if not tries < max_retries:
                    raise Error(e)
                else:
                    time.sleep(60)

    def head_file(self, file_uuid, version=None):
        # finally create the bundle
        try:
            return self.hca_client.head_file(
                uuid=file_uuid,
                version=version,
                replica="aws"
            )
        except Exception as e:
            raise Error(e)


# Module Exceptions

class Error(Exception):
    """Base-class for all exceptions raised by this module."""