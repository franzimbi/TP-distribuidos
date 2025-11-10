import logging
import signal
import sys
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
from configparser import ConfigParser
import socket
import threading

config = ConfigParser()
config.read("config.ini")

HEALTH_PORT = 3030

class Counter:
    def __init__(self, consumer, producer, *, key_columns, count_name):
        self._consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self._producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)
        self.key_columns = list(key_columns)
        self.count_name = count_name

        self._health_sock = None
        self._health_thread = None

        signal.signal(signal.SIGTERM, self.graceful_quit)

    def graceful_quit(self, signum, frame):
        logging.debug("Recibida señal SIGTERM, cerrando counter...")
        self.stop()
        logging.debug("Counter cerrado correctamente.")
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
        self._consumer_queue.start_consuming(self.callback)

    def stop(self):
        self._consumer_queue.stop_consuming()
        self._consumer_queue.close()
        self._producer_queue.close()

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)

        try:
            if batch.is_last_batch():
                self._producer_queue.send(batch.encode())
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            accumulator = {}
            for row in batch.iter_per_header():
                try:
                    key = tuple(str(row[col]).strip() for col in self.key_columns)
                    if any(k == "" for k in key):
                        continue
                    accumulator[key] = accumulator.get(key, 0) + 1
                except Exception as e:
                    logging.warning(f"[COUNTER] Malformed row: {row} | Error: {e}")
                    continue

            header = self.key_columns + [self.count_name]
            rows = [[*key, str(cnt)] for key, cnt in accumulator.items()]

            out = Batch(
                id=batch.id(),
                query_id=batch.get_query_id(),
                client_id=batch.client_id(),
                last=False,
                type_file=batch.type(),
                header=header,
                rows=rows
            )
            self._producer_queue.send(out.encode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logging.exception("[COUNTER] Error procesando batch")
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                logging.exception("[COUNTER] Error al hacer basic_nack")
