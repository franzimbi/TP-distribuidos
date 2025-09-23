#!/usr/bin/env python3
import os

from middleware.middleware import MessageMiddlewareQueue

queue_consumer = os.getenv("CONSUME_QUEUE")
consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)

def callback(ch, method, properties, body):
    print("\n\n\n\nllego esto: %r\n\n\n\n\n" % body)

try:
    consumer.start_consuming(callback)
except KeyboardInterrupt:
    consumer.stop_consuming()
    consumer.close()
