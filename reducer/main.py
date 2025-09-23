#!/usr/bin/env python3
import os
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import decode_batch

queue_consumer = os.getenv("CONSUME_QUEUE")
consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)

def callback(ch, method, properties, body):
    if body == b"&END&":
        print("\n\n[REDUCER] Recibido END, fin de procesamiento.\n\n")
        return

    batch = decode_batch(body)
    print(f"\n\n[REDUCER] Llegó batch: {batch}\n\n")

try:
    consumer.start_consuming(callback)
except KeyboardInterrupt:
    consumer.stop_consuming()
    consumer.close()
