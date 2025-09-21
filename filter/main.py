#!/usr/bin/env python3
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

channel = connection.channel()
channel.queue_declare(queue='filterq1')


def callback(ch, method, properties, body):
    print("llego: %r" % body)

channel.basic_consume(
    queue='filterq1', on_message_callback=callback, auto_ack=True)
channel.start_consuming()