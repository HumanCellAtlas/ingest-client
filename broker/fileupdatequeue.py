#!/usr/bin/env python

import pika
import json
# we need to agree what other meta data will provide e.g. file size, checksums etc..
fileMessage = {
    "cloudUrl" : "<URL to staging area>",
    "fileName" : "<name of file uploaded to staging area",
    "envelopeUuid" : "<envelope id that links the staging are to submission"
}

connection = pika.BlockingConnection(pika.ConnectionParameters('amqp.ingest.dev.data.humancellatlas.org'))
channel = connection.channel()
channel.queue_declare(queue='ingest.file.create.staged')
channel.basic_publish(exchange='',
                      routing_key='ingest.file.create.staged',
                      body=json.dumps(fileMessage))
connection.close()
