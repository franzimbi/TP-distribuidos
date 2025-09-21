#!/usr/bin/env python3
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='filterq1')

for i in range(5):
    channel.basic_publish(exchange='', routing_key='filterq1', body='hola Peter Buu{}'.format(i))
    time.sleep(5)

connection.close()