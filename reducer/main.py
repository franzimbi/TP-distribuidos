#!/usr/bin/env python3
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

channel = connection.channel()
channel.queue_declare(queue='reducerq1', durable=True)


def callback(ch, method, properties, body):
    print("reduje esto: %r" % body)

channel.basic_consume(
    queue='reducerq1', on_message_callback=callback, auto_ack=True)
channel.start_consuming()