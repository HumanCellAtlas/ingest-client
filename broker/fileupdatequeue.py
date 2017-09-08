#!/usr/bin/env python

import pika
import json
# we need to agree what other metadata staging   will provide e.g. file size, checksums etc..
fileMessage = {
    "cloudUrl" : "s3://bucket/706161df-b2c9-4e12-ba34-169faaf5137b/ERR1630013.fastq.gz",
    "fileName" : "ERR1630013.fastq.gz",
    "envelopeUuid" : { "uuid" :"706161df-b2c9-4e12-ba34-169faaf5137b"}

}

connection = pika.BlockingConnection(pika.ConnectionParameters('amqp.ingest.dev.data.humancellatlas.org'))
channel = connection.channel()
channel.queue_declare(queue='ingest.file.create.staged')
channel.basic_publish(exchange='ingest.file.staged.exchange',
                      routing_key='ingest.file.create.staged',
                      body=json.dumps(fileMessage))
connection.close()
