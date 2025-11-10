import time

from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import logging
import signal
import sys
import socket
import threading

from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])
HEALTH_PORT = 3030

class Filter:
    def __init__(self, consume_queue, produce_queue, filter):
        self._buffer = Batch()
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)

        self._produce_queues = []
        for queue_name in produce_queue.split(','):
            queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_name)
            self._produce_queues.append(queue)

        self._filter = filter

        self._health_sock = None
        self._health_thread = None

        signal.signal(signal.SIGTERM, self.graceful_shutdown)


    def graceful_shutdown(self, signum, frame):
        try:
            logging.debug("Recibida señal SIGTERM, cerrando filter...")
            self.close()
        except Exception as e:
            logging.error(f"[FILTER] Error al cerrar: {e}")
        sys.exit(0)

    def start(self):
        self._health_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._health_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._health_sock.bind(('', HEALTH_PORT))
        self._health_sock.listen()

        def loop():
            while True:
                conn, addr = self._health_sock.accept()
                conn.close()

        self._health_thread = threading.Thread(target=loop, daemon=True)
        self._health_thread.start()
        logging.info(f"[FILTER] Healthcheck escuchando en puerto {HEALTH_PORT}")

        self._consume_queue.start_consuming(self.callback, auto_ack=False)

    def callback(self, ch, method, properties, message):
        try:
            batch = Batch(); batch.decode(message)
            # print(f"[FILTER] Recibido batch con {batch.id()}")
            if not batch.is_last_batch():
                batch = self._filter(batch)
            for q in self._produce_queues:
                q.send(batch.encode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logging.exception("[FILTER] Error al procesar el batch")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def close(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        for queue in self._produce_queues:
            queue.close()
        logging.debug("Queues cerradas")
        logging.debug("[FILTER] Apagado limpio.")
