#!/usr/bin/env python3
import os
from middleware.middleware import MessageMiddlewareQueue
from filters import *
from filter import Filter
import logging

filter_type = str(os.getenv("FILTER_NAME"))
queue_consumer = str(os.getenv("CONSUME_QUEUE"))
queue_producer = str(os.getenv("PRODUCE_QUEUE"))
coordinator_consumer = str(os.getenv("COORDINATOR_CONSUME_QUEUE"))
coordinator_producer = str(os.getenv("COORDINATOR_PRODUCE_QUEUE"))


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=os.getenv('LOGGING_LEVEL'),
    datefmt='%Y-%m-%d %H:%M:%S',)

logging.getLogger("pika").setLevel(logging.CRITICAL)

filters = {
    'bytime': filter_by_time,
    'byamount': filter_by_amount,
    'bycolumn': filter_by_column,
    'byyear': filter_by_2024_2025
}

# consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
# producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)
# coordinator_consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_consumer)
# coordinator_producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_producer)

# print(f"[{filter_type}] Escuchando en cola: {queue_consumer}, enviando a: {queue_producer}")

filter_by_env = filters[filter_type]
this_filter = Filter(queue_consumer, queue_producer, filter_by_env, coordinator_consumer, coordinator_producer)
this_filter.start()
