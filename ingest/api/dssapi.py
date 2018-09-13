#!/usr/bin/env python
import datetime
import hca
import json
import logging
import os

from hca.util import RetryPolicy


class DssApi:
    def __init__(self, url=None):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        self.logger = logging.getLogger(__name__)

        self.url = url if url else 'https://dss.dev.data.humancellatlas.org'
        if not url and 'DSS_API' in os.environ:
            url = os.environ['DSS_API']
            # expand interpolated env vars
            self.url = os.path.expandvars(url)
            self.logger.info(f'using {url} for dss API')

        self.headers = {'Content-type': 'application/json'}

        self.hca_client = hca.dss.DSSClient()
        retry_policy = RetryPolicy(read=10, status=15, status_forcelist=frozenset({500, 502, 503, 504}), backoff_factor=0.3)
        self.hca_client.retry_policy = retry_policy
        self.hca_client.host = self.url + '/v1'
        self.creator_uid = 8008

    def put_file(self, bundle_uuid, file):
        url = file['url']
        uuid = file['dss_uuid']

        version = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H%M%S.%fZ')

        try:
            bundle_file = self.hca_client.put_file(
                uuid=uuid,
                version=version,
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
            self.logger.error(f'Error in hca_client.put_file method call with params:{json.dumps(params)}')

            raise

        return bundle_file

    def put_bundle(self, bundle_uuid, bundle_files):
        bundle = None

        # Generate version client-side for idempotent PUT /bundle
        version = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H%M%S.%fZ')

        try:
            bundle = self.hca_client.put_bundle(
                uuid=bundle_uuid,
                version=version,
                replica="aws",
                files=bundle_files,
                creator_uid=self.creator_uid
            )
            return bundle
        except Exception as e:
            params = {
                'uuid': bundle_uuid,
                'version': version,
                'replica': "aws",
                'files': bundle_files,
                'creator_uid': self.creator_uid
            }
            self.logger.error(f'Error in hca_client.put_bundle method call with params: {json.dumps(params)}')

            raise

    def head_file(self, file_uuid):
        # finally create the bundle
        try:
            return self.hca_client.head_file(
                uuid=file_uuid,
                replica='aws'
            )
        except Exception as e:
            raise Error(e)


# Module Exceptions


class Error(Exception):
    """Base-class for all exceptions raised by this module."""
