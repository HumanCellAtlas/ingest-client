#!/usr/bin/env python
"""
Description goes here
"""
import datetime
import hca
import json
import logging
import os


__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "12/09/2017"


class DssApi:
    def __init__(self, url=None):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)

        self.url = url if url else "http://dss.dev.data.humancellatlas.org"

        if not url and 'DSS_API' in os.environ:
            url = os.environ['DSS_API']
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info("using " + url + " for dss API")

        self.headers = {'Content-type': 'application/json'}

        self.hca_client = hca.dss.DSSClient()
        self.hca_client.host = self.url + "/v1"
        self.creator_uid = 8008

    def put_file(self, bundle_uuid, file):
        url = file["url"]
        uuid = file["dss_uuid"]

        try:
            bundle_file = self.hca_client.put_file(
                uuid=uuid,
                bundle_uuid=bundle_uuid,
                creator_uid=self.creator_uid,
                source_url=url
            )
        except Exception as e:
            params = {
                'uuid': uuid,
                'bundle_uuid': bundle_uuid,
                'creator_uid': self.creator_uid,
                'source_url': url
            }
            self.logger.error('Error in hca_client.put_file method call with params:' + json.dumps(params))
            raise Error(e)

        return bundle_file

    def put_bundle(self, bundle_uuid, bundle_files):
        bundle = None

        # Generate version client-side for idempotent PUT /bundle
        version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

        # finally create the bundle
        try:
            bundle = self.hca_client.put_bundle(
                uuid=bundle_uuid,
                version=version,
                replica="aws",
                files=bundle_files,
                creator_uid=self.creator_uid
            )
        except Exception as e:
            params = {
                'uuid': bundle_uuid,
                'version': version,
                'replica': "aws",
                'files': bundle_files,
                'creator_uid': self.creator_uid
            }
            self.logger.error('Error in hca_client.put_bundle method call with params:' + json.dumps(params))
            raise Error(e)

        return bundle

    def head_file(self, file_uuid ):
        # finally create the bundle
        try:
            return self.hca_client.head_file(
                uuid=file_uuid,
                replica="aws"
            )
        except Exception as e:
            raise Error(e)


# Module Exceptions


class Error(Exception):
    """Base-class for all exceptions raised by this module."""
