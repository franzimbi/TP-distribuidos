#!/usr/bin/env python3
import os
from middleware.middleware import MessageMiddlewareQueue
from filters import *

filter_type = os.getenv("FILTER_NAME")
queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")

filters = {
    'bytime': filter_by_time,
    'byamount': filter_by_amount
}

def decode_to_batch(data: bytes) -> list[list[str]]:
    batch_str = data.decode("utf-8")
    data_list = batch_str.strip().split(",")
    return [data_list]

def callback(ch, method, properties, message):
    batch = decode_to_batch(message)
    result = filters[filter_type](batch)
    print(f"[FILTER] Recibido: {batch}, enviado: {result}")
    if result:
        batch_str = "\n".join([",".join(row) for row in result])
        producer.send(batch_str)

consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)

try:
    consumer.start_consuming(callback)
except KeyboardInterrupt:
    consumer.stop_consuming()
    consumer.close()
    producer.close()
