#!/usr/bin/env python3
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

channel = connection.channel()
channel.queue_declare(queue='filterq1', durable=True)
channel.queue_declare(queue='reducerq1', durable=True)

def callback(ch, method, properties, body):
    print("llego: %r" % body)
    body = body.decode('utf-8').upper()
    channel.basic_publish(exchange='', routing_key='reducerq1', body=body)

channel.basic_consume(
    queue='filterq1', on_message_callback=callback, auto_ack=True)
channel.start_consuming()