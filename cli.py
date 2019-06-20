#!/usr/bin/env python
import datetime
import logging
import os
import sys
import uuid

from optparse import OptionParser

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi
from ingest.api.stagingapi import StagingApi
from ingest.exporter.ingestexportservice import IngestExporter
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

if __name__ == '__main__':
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=format, stream=sys.stdout, level=logging.INFO)

    parser = OptionParser()
    parser.add_option("-b", "--bundle-uuid",
                      help="Bundle UUID (optional)")
    parser.add_option("-v", "--bundle-version",
                      help="Bundle Version (optional)")
    parser.add_option("-e", "--submission-uuid",
                      help="Submission envelope UUID for which to generate the bundle (Required)")
    parser.add_option("-p", "--process-uuid",
                      help="Process UUID (Required)")
    parser.add_option("-D", "--dry", help="do a dry run without submitting to ingest", action="store_true",
                      default=False)
    parser.add_option("-o", "--output", dest="output",
                      help="Output directory where to dump json files submitted to ingest (Optional)", metavar="FILE",
                      default=None)
    parser.add_option("-i", "--ingest", help="the URL to the ingest API (Required)")
    parser.add_option("-s", "--staging", help="the URL to the staging API")
    parser.add_option("-d", "--dss", help="the URL to the datastore service")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')

    (options, args) = parser.parse_args()

    if not options.submission_uuid:
        print("You must supply a Submission Envelope UUID")
        exit(2)

    if not options.process_uuid:
        print("You must supply a process UUID.")
        exit(2)

    # TODO must only ask which environment to use
    if not options.ingest:
        print("You must the url of Ingest API.")
        exit(2)

    bundle_uuid = str(uuid.uuid4())
    bundle_version = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

    if options.bundle_uuid:
        bundle_uuid = options.bundle_uuid

    if options.bundle_version:
        bundle_version = options.bundle_version

    staging_api = StagingApi(url=options.staging)
    dss_api = DssApi(url=options.dss)
    s2s_token_client = S2STokenClient()
    gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    s2s_token_client.setup_from_file(gcp_credentials_file)
    token_manager = TokenManager(token_client=s2s_token_client)
    ingest_api = IngestApi(token_manager=token_manager)
    exporter = IngestExporter(staging_api=staging_api, ingest_api=ingest_api, dss_api=dss_api)
    exporter.export_bundle(bundle_uuid=bundle_uuid,
                           bundle_version=bundle_version,
                           submission_uuid=options.submission_uuid,
                           process_uuid=options.process_uuid)
