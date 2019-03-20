#!/usr/bin/env python

# example cli run:
# python cli.py --submissionEnvelopeUuid=21ec3581-abc2-4e1c-9fd4-abce09aa5c19 --processUuid=b3f91034-f0de-4fb0-a038-b011b9f231b6 --ingest=http://api.ingest.dev.data.humancellatlas.org --dry
import datetime
import logging
import sys
import uuid

from optparse import OptionParser

from ingest.exporter.ingestexportservice import IngestExporter

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

    exporter = IngestExporter(options)
    exporter.export_bundle(bundle_uuid=bundle_uuid,
                           bundle_version=bundle_version,
                           submission_uuid=options.submission_uuid,
                           process_uuid=options.process_uuid)
