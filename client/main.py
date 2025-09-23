#!/usr/bin/env python3
import os
import time

from middleware.middleware import MessageMiddlewareQueue
from common.protocol import make_batches_from_csv

queue_producer = os.getenv("PRODUCE_QUEUE")

mw = MessageMiddlewareQueue(host='rabbitmq', queue_name=queue_producer)

make_batches_from_csv('trans.csv', 5, mw)

# connection.close() #hay q cerrar la connection en el middleware
mw.close()