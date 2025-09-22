#!/usr/bin/env python3
import os
import pika

from filters import *

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))


filter_type = os.getenv("FILTER_NAME")
queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")

channel = connection.channel()
channel.queue_declare(queue=queue_consumer, durable=True)
channel.queue_declare(queue=queue_producer, durable=True)

filters = {
    'bytime': filter_by_time,
    'byamount': filter_by_amount
}

def decode_to_batch(data: bytes) -> list[list[str]]:
    batch_str = data.decode("utf-8")
    data_list = batch_str.strip().split(",")
    return [data_list]


def callback(ch, method, properties, body):
    # batch = decode_to_batch(body)
    batch = decode_to_batch(body)
    result = filters[filter_type](batch)
    print(f"[FILTER1] Recibido: {batch}, enviado: {result}")
    if result:
        batch_str = "\n".join([",".join(row) for row in result])
        channel.basic_publish(exchange='', routing_key=queue_producer, body=batch_str)


channel.basic_consume(
    queue=queue_consumer, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
