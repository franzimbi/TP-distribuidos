#!/usr/bin/env python3
import pika
import time
from middleware.middleware import MessageMiddlewareQueue


mw = MessageMiddlewareQueue(host='rabbitmq', queue_name='filterq1')


# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host='rabbitmq'))

# channel = connection.channel()

# channel.queue_declare(queue='filterq1')

with open('stores.csv', 'r') as stores:
    for line in stores:
        # channel.basic_publish(exchange='', routing_key='filterq1', body=line.strip())
        mw.send(line.strip())
        time.sleep(3)

# for i in range(5):
#     channel.basic_publish(exchange='', routing_key='filterq1', body='hola Peter Buu{}'.format(i))
#     time.sleep(5)

# connection.close() #hay q cerrar la connection en el middleware
mw.close()