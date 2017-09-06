import pika
import broker.ingestexportservice
import broker.ingestapi
from optparse import OptionParser
import os
import logging

# this script listens on a ingest submission queue and as submission are completed will
# call the ingest export service to generate the bundles and submit bundles to datastore

DEFAULT_RABBIT_URL=os.environ.get('RABBIT_URL', 'amqp://localhost:5672')
DEFAULT_INGEST_URL=os.environ.get('INGEST_API', 'http://localhost:8080')
DEFAULT_QUEUE_NAME=os.environ.get('SUBMISSION_QUEUE_NAME', 'ingest.envelope.submitted.queue')

class IngestReceiver:
    def __init__(self, options={}):

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(level=options.log, formatter=formatter)

        self.ingestUrl = options.ingest if options.ingest else DEFAULT_INGEST_URL
        logging.debug("ingest url is "+self.ingestUrl )

        self.rabbit = options.rabbit if options.rabbit else os.path.expandvars(DEFAULT_RABBIT_URL)
        logging.debug("rabbit url is "+self.rabbit )

        self.queue = options.queue if options.queue else DEFAULT_QUEUE_NAME
        logging.debug("rabbit queue is "+self.queue )

        connection = pika.BlockingConnection(pika.URLParameters(self.rabbit))
        channel = connection.channel()

        channel.queue_declare(queue=self.queue)

        def callback(ch, method, properties, body):
            logging.info(" [x] Received %r" % body)
            if "id" in body:
                ingestExporter = broker.ingestexportservice.IngestExporter()
                ingestExporter.generateBundles(id)

        channel.basic_consume(callback,
                              queue=self.queue,
                              no_ack=True)

        logging.info(' [*] Waiting for messages from submission envelope')
        channel.start_consuming()


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-q", "--queue", help="name of the ingest queues to listen for submission")
    parser.add_option("-r", "--rabbit", help="the URL to the Rabbit MQ messaging server")
    parser.add_option("-i", "--ingest", help="the URL to the ingest API")
    parser.add_option("-s", "--staging", help="the URL to the staging API")
    parser.add_option("-d", "--dss", help="the URL to the datastore service")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')

    (options, args) = parser.parse_args()
    IngestReceiver(options)
