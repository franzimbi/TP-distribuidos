from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import logging
import signal
import sys

from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])

class Filter:
    def __init__(self, consume_queue, produce_queue, filter):
        self._buffer = Batch()
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)

        self._produce_queues = []
        for queue_name in produce_queue.split(','):
            queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_name)
            self._produce_queues.append(queue)

        self._filter = filter
        signal.signal(signal.SIGTERM, self.graceful_shutdown)


    def graceful_shutdown(self, signum, frame):
        try:
            logging.debug("Recibida señal SIGTERM, cerrando filter...")
            self.close()
        except Exception as e:
            logging.error(f"[FILTER] Error al cerrar: {e}")
        sys.exit(0)

    def start(self):
        self._consume_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)
        if not batch.is_last_batch():
            batch = self._filter(batch)
        for q in self._produce_queues:
            q.send(batch.encode())
        

    def close(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        for queue in self._produce_queues:
            queue.close()
        logging.debug("Queues cerradas")
        logging.debug("[FILTER] Apagado limpio.")
