#!/usr/bin/env python
"""
Description goes here
"""
import datetime
import json
import logging
import os
import time

import hca
from hca.util import SwaggerAPIException

from ingest.api import utils
from ingest.utils.token_manager import Token

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

        self.dss_client = None

        self.token = None
        self.init_dss_client()

    # TODO This is workaround to not let the dss client token expired
    # See ticket https://app.zenhub.com/workspaces/dcp-5ac7bcf9465cb172b77760d9/issues/humancellatlas/data-store/2216
    # Create a dummy token at the same time DSS client is initialised.
    # This is just a way to keep track when DSS client token will be expired
    def init_dss_client(self, duration=3600 * 1000, refresh_period=60 * 10 * 1000):
        if not self.token or self.token.is_expired():
            self.token = Token('dummy', duration, refresh_period)
            self.dss_client = hca.dss.DSSClient(
                swagger_url=f'{self.url}/v1/swagger.json')
            self.dss_client.host = self.url + "/v1"
            self.creator_uid = 8008

    def put_file(self, bundle_uuid, file):
        url = file["url"]
        uuid = file["dss_uuid"]

        update_date = file.get("update_date")
        version = utils.to_dss_version(update_date) if update_date \
            else datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        # retrying file creation 20 times
        max_retries = 20
        tries = 0
        file_create_complete = False

        params = {
            'uuid': uuid,
            'version': version,
            'creator_uid': self.creator_uid,
            'source_url': url
        }

        if bundle_uuid:
            params["bundle_uuid"] = bundle_uuid

        while not file_create_complete and tries < max_retries:
            try:
                tries += 1
                self.logger.info(f'Creating file in DSS {uuid}:{version} with params: {json.dumps(params)}')
                bundle_file = None

                if bundle_uuid:
                    self.init_dss_client()
                    bundle_file = self.dss_client.put_file(
                        uuid=uuid,
                        version=version,
                        bundle_uuid=bundle_uuid,
                        creator_uid=self.creator_uid,
                        source_url=url
                    )
                else:
                    self.init_dss_client()
                    bundle_file = self.dss_client.put_file(
                        uuid=uuid,
                        version=version,
                        creator_uid=self.creator_uid,
                        source_url=url
                    )

                self.logger.info('Created!')
                file_create_complete = True
                return bundle_file
            except Exception as e:
                self.logger.error(
                    'Attempt {0} out of {1}: Error in hca_client.put_file method call with params:{2} due to {'
                    '3}'.format(
                        str(tries),
                        str(max_retries),
                        json.dumps(params),
                        str(e))
                )

                if not tries < max_retries:
                    raise Error(e)
                else:
                    time.sleep(60)

    def put_bundle(self, bundle_uuid, version, bundle_files):
        bundle = None

        # retrying file creation 20 times
        max_retries = 20
        tries = 0
        bundle_create_complete = False

        # finally create the bundle
        while not bundle_create_complete and tries < max_retries:
            try:
                tries += 1
                self.logger.info(f'Creating bundle in DSS {bundle_uuid}:{version}')
                self.init_dss_client()
                bundle = self.dss_client.put_bundle(
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
                if isinstance(e, SwaggerAPIException):
                    error_code = e.details.get('code')

                    if error_code.lower() == 'bundle_already_exists':
                        raise BundleAlreadyExist(f"Bundle {bundle_uuid}.{version} already exist in DSS.")

                params = {
                    'uuid': bundle_uuid,
                    'version': version,
                    'replica': "aws",
                    'files': bundle_files,
                    'creator_uid': self.creator_uid
                }
                self.logger.error(
                    'Attempt {0} out of {1}: Error in hca_client.put_bundle method call with params:{2} due to {'
                    '3}'.format(
                        str(tries),
                        str(max_retries),
                        json.dumps(params),
                        str(e))
                )
                if not tries < max_retries:
                    raise Error(e)
                else:
                    time.sleep(60)

    def get_bundle(self, bundle_uuid, version=None):
        self.init_dss_client()
        if version:
            return self.dss_client.get_bundle(
                uuid=bundle_uuid,
                replica="aws",
                version=version
            )
        else:
            return self.dss_client.get_bundle(
                uuid=bundle_uuid,
                replica="aws"
            )

    def get_file(self, file_uuid, version=None):
        self.init_dss_client()
        if version:
            return self.dss_client.get_file(
                    uuid=file_uuid,
                    replica="aws",
                    version=version
            )
        else:
            return self.dss_client.get_file(
                    uuid=file_uuid,
                    replica="aws"
            )

    def head_file(self, file_uuid, version=None):
        self.init_dss_client()
        # finally create the bundle
        try:
            return self.dss_client.head_file(
                uuid=file_uuid,
                version=version,
                replica="aws"
            )
        except Exception as e:
            raise Error(e)


# Module Exceptions

class Error(Exception):
    """Base-class for all exceptions raised by this module."""


class BundleAlreadyExist(Error):
    """Bundle in DSS already Exist."""
