#!/usr/bin/env python3
import os
from middleware.middleware import MessageMiddlewareQueue
from filters import *
from filter import Filter

filter_type = os.getenv("FILTER_NAME")
queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")

coordinator_consumer =  os.getenv("COORDINATOR_CONSUME_QUEUE", "coordinator_to_filter")
coordinator_producer =  os.getenv("COORDINATOR_PRODUCE_QUEUE", "filter_to_coordinator")



filters = {
    'bytime': filter_by_time,
    'byamount': filter_by_amount,
    'bycolumn': filter_by_column
}

consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)

coordinator_consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_consumer)
coordinator_producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_producer)

print(f"[{filter_type}] Escuchando en cola: {queue_consumer}, enviando a: {queue_producer}")

try:
    filter_by_env = filters[filter_type]
    this_filter = Filter(queue_consumer, queue_producer, filter_by_env, coordinator_consumer, coordinator_producer)
    this_filter.start()
except KeyboardInterrupt:
    coordinator_consumer_queue.stop_consuming()
    coordinator_consumer_queue.close()
    coordinator_producer_queue.close()
    consumer.stop_consuming()
    consumer.close()
    producer.close()
