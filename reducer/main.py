#!/usr/bin/env python3
import os

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

queue_consumer = os.getenv("CONSUME_QUEUE")
channel = connection.channel()
channel.queue_declare(queue=queue_consumer, durable=True)


def callback(ch, method, properties, body):
    print("\n\n\n\nllego esto: %r\n\n\n\n\n" % body)

channel.basic_consume(
    queue=queue_consumer, on_message_callback=callback, auto_ack=True)
channel.start_consuming()