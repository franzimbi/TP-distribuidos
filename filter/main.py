#!/usr/bin/env python3
import os
from middleware.middleware import MessageMiddlewareQueue
from filters import *
from filter import Filter

filter_type = os.getenv("FILTER_NAME")
queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")

filters = {
    'bytime': filter_by_time,
    'byamount': filter_by_amount,
    'bycolumn': filter_by_column
}

consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)

print(f"[{filter_type}] Escuchando en cola: {queue_consumer}, enviando a: {queue_producer}")

try:
    filter_by_env = filters[filter_type]
    this_filter = Filter(queue_consumer, queue_producer, filter_by_env)
    this_filter.start()
except KeyboardInterrupt:
    consumer.stop_consuming()
    consumer.close()
    producer.close()
