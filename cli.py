#!/usr/bin/env python

# example cli run:
# python cli.py --submissionEnvelopeUuid=21ec3581-abc2-4e1c-9fd4-abce09aa5c19 --processUuid=b3f91034-f0de-4fb0-a038-b011b9f231b6 --ingest=http://api.ingest.dev.data.humancellatlas.org --dry

import logging
import sys

from optparse import OptionParser

from ingest.exporter.ingestexportservice import IngestExporter

if __name__ == '__main__':
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=format, stream=sys.stdout, level=logging.INFO)

    parser = OptionParser()
    parser.add_option("-e", "--submissionEnvelopeUuid",
                      help="Submission envelope UUID for which to generate the bundle")
    parser.add_option("-p", "--processUuid",
                      help="Process UUID")
    parser.add_option("-D", "--dry", help="do a dry run without submitting to ingest", action="store_true",
                      default=False)
    parser.add_option("-o", "--output", dest="output",
                      help="output directory where to dump json files submitted to ingest", metavar="FILE",
                      default=None)
    parser.add_option("-i", "--ingest", help="the URL to the ingest API")
    parser.add_option("-s", "--staging", help="the URL to the staging API")
    parser.add_option("-d", "--dss", help="the URL to the datastore service")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')

    (options, args) = parser.parse_args()

    if not options.submissionEnvelopeUuid:
        print ("You must supply a Submission Envelope UUID")
        exit(2)

    if not options.processUuid:
        print ("You must supply a process UUID.")
        exit(2)

    # TODO must only ask which environment to use
    if not options.ingest:
        print ("You must the url of Ingest API.")
        exit(2)

    exporter = IngestExporter(options)
    exporter.export_bundle(options.submissionEnvelopeUuid, options.processUuid)
