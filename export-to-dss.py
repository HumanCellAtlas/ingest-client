#!/usr/bin/env python
"""
This script listens on a ingest submission queue and as submission are completed will
call the ingest export service to generate the bundles and submit bundles to datastore
"""
__author__ = "jupp"
__license__ = "Apache 2.0"

import pika
import broker.ingestexportservice
import broker.ingestapi
from optparse import OptionParser
import os, sys
import logging
import json

DEFAULT_RABBIT_URL=os.environ.get('RABBIT_URL', 'amqp://localhost:5672')
DEFAULT_QUEUE_NAME=os.environ.get('SUBMISSION_QUEUE_NAME', 'ingest.envelope.submitted.queue')

class IngestReceiver:
    def __init__(self, options={}):

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(level=options.log, formatter=formatter)
        self.logger = logging.getLogger(__name__)

        self.rabbit = options.rabbit if options.rabbit else os.path.expandvars(DEFAULT_RABBIT_URL)
        self.logger.debug("rabbit url is "+self.rabbit )

        self.queue = options.queue if options.queue else DEFAULT_QUEUE_NAME
        self.logger.debug("rabbit queue is "+self.queue )

        connection = pika.BlockingConnection(pika.URLParameters(self.rabbit))
        channel = connection.channel()

        channel.queue_declare(queue=self.queue)

        def callback(ch, method, properties, body):
            self.logger.info(" [x] Received %r" % body)
            submittedObject = json.loads(body)
            if "id" in submittedObject:
                ingestExporter = broker.ingestexportservice.IngestExporter()
                ingestExporter.generateBundles(submittedObject["id"])

        channel.basic_consume(callback,
                              queue=self.queue,
                              no_ack=True)

        self.logger.info(' [*] Waiting for messages from submission envelope')
        channel.start_consuming()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = OptionParser()
    parser.add_option("-q", "--queue", help="name of the ingest queues to listen for submission")
    parser.add_option("-r", "--rabbit", help="the URL to the Rabbit MQ messaging server")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')

    (options, args) = parser.parse_args()
    IngestReceiver(options)
