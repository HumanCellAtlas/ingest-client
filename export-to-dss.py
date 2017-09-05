import pika
import broker.ingestexportservice
import broker.ingestapi
from optparse import OptionParser

# this script listens on a ingest submission queue and as submission are completed will
# call the ingest export service to generate the bundles and submit bundles to datastore

class IngestReceiver:
    def __init__(self, host=None, port=None, queue=None):

        self.queue = queue if queue else 'ingest.envelope.submitted.queue'
        self.host = host if host else 'localhost'
        self.port = port if port else 5672

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        channel = connection.channel()

        channel.queue_declare(queue=self.queue)

        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)
            if "id" in body:
                ingestExporter = broker.ingestexportservice.IngestExporter()
                ingestExporter.generateBundles(id)

        channel.basic_consume(callback,
                              queue=self.queue,
                              no_ack=True)

        print(' [*] Waiting for messages from submission envelope')
        channel.start_consuming()


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-q", "--queue", help="name of the ingest queus to listen for submission")
    parser.add_option("-mh", "--mhost", help="the host where the Rabbit MQ messaging server is running")
    parser.add_option("-mp", "--mport", help="the port where the Rabbit MQ messaging server is running")

    (options, args) = parser.parse_args()
    IngestReceiver(args)
