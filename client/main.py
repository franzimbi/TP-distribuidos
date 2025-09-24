#!/usr/bin/env python3
import os
import time

from middleware.middleware import MessageMiddlewareQueue
from common.protocol import make_batches_from_csv

queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")


producerQ1 = MessageMiddlewareQueue(host='rabbitmq', queue_name=queue_producer)
consumerQ1 = MessageMiddlewareQueue(host='rabbitmq', queue_name=queue_consumer)
fileQ1 = open('results/q1.csv', 'w')
fileQ3 = open('results/q3.csv', 'w')
counter = 0

make_batches_from_csv('csvs_files/transactions', 150, producerQ1)


def callbackQ1(ch, method, properties, message):
    global counter
    data = message.decode("utf-8")

    if data == "&END&":
        print("Recibido END, deteniendo consumo...")
        consumerQ1.stop_consuming()
        return

    for line in data.split('|'):
        counter += 1
        fileQ1.write(line+'\n')

def callbackQ3(ch, method, properties, message):
    pass


consumerQ1.start_consuming(callbackQ1)
#consumer2.stop_consuming(callbackQ3)

# connection.close() #hay q cerrar la connection en el middleware
print(f"Q1: {counter} rows received\n\n\n\n")
producerQ1.close()
consumerQ1.close()
