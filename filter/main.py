#!/usr/bin/env python3
import os
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import decode_batch
from filters import *

filter_type = os.getenv("FILTER_NAME")
queue_consumer = os.getenv("CONSUME_QUEUE")
queue_producer = os.getenv("PRODUCE_QUEUE")
BUFFER_SIZE = 5
END_MARKER = b"&END&"

filters = {
    'bytime': filter_by_time,
    'byamount': filter_by_amount
}

filter_id = filter_type.upper()

count = 0
aux = ""

consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)

print(f"[{filter_id}] Escuchando en cola: {queue_consumer}, enviando a: {queue_producer}")

def _send_current_buffer():
    global aux, count
    if count > 0:
        print(f"[{filter_id}] Publicado en {queue_producer}: {aux}")
        producer.send(aux)
        aux = ""
        count = 0

def callback(ch, method, properties, message):
    global aux, count

    if message == END_MARKER:
        print(f"[{filter_id}] Recibido END de {queue_consumer}, finalizando.")
        _send_current_buffer()
        if queue_producer:
            producer.send(END_MARKER)
        return

    batch = decode_batch(message)
    result = filters[filter_type](batch)
    print(f"[{filter_id}] Recibido: {batch}")
    print(f"[{filter_id}] Filtrado: {result}")

    if not result:
        return

    for row in result:
        line = ",".join(row)
        aux = (aux + "|" + line) if aux else line
        count += 1
        if count >= BUFFER_SIZE:
            _send_current_buffer()

try:
    consumer.start_consuming(callback)
except KeyboardInterrupt:
    consumer.stop_consuming()
    consumer.close()
    producer.close()