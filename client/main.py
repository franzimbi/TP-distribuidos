#!/usr/bin/env python3
import os
import time

from middleware.middleware import MessageMiddlewareQueue
from common.protocol import make_batches_from_csv

queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")

producer = MessageMiddlewareQueue(host='rabbitmq', queue_name=queue_producer)
consumer = MessageMiddlewareQueue(host='rabbitmq', queue_name=queue_consumer)
file = open('results/q1.csv', 'w')
counter = 0

make_batches_from_csv('csvs_files/transactions', 150, producer)


def callback(ch, method, properties, message):
    global counter
    for line in message.decode("utf-8").split('|'):
        counter += 1
        file.write(line+'\n')


consumer.start_consuming(callback)

# connection.close() #hay q cerrar la connection en el middleware
print(f"Q1: {counter} rows received\n\n\n\n/n/n/n")
producer.close()
consumer.close()
