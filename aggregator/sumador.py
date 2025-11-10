import logging
import signal
import sys
from datetime import datetime
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
from configparser import ConfigParser
import socket
import threading

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])
HEALTH_PORT = 3030

def year_month(ts: str) -> str:
    d = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return f"{d.year}_{d.month:02d}"

def year_semester(ts: str) -> str:
    d = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return f"{d.year}_{'S1' if d.month <= 6 else 'S2'}"

BUCKET = {
    "month": year_month,
    "semester": year_semester,
}

class Adder:
    def __init__(self, consume_queue, produce_queue, *,
                 key_col: str,
                 value_col: str,
                 bucket_kind: str,
                 bucket_name: str,
                 time_col: str = "created_at",
                 out_value_name: str = "tpv"):
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)

        self._key_col = key_col
        self._value_col = value_col
        self._time_col = time_col
        self._out_value_name = out_value_name

        self._health_sock = None
        self._health_thread = None

        kind = bucket_kind.lower().strip()
        self._bucket_fn = BUCKET[kind]
        self._bucket_name = bucket_name.strip()
        
        signal.signal(signal.SIGTERM, self.graceful_quit)

    def graceful_quit(self, signum, frame):
        logging.debug("Recibida señal SIGTERM, cerrando aggregator...")
        self.stop()
        logging.debug("Aggregator cerrado correctamente.")
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
        logging.info(f"[SUMADOR] Healthcheck escuchando en puerto {HEALTH_PORT}")


        self._consume_queue.start_consuming(self.callback)

    def stop(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        self._produce_queue.close()
        

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)
        # print(f"[SUMADOR] Recibido batch con {batch.id()}")
        try:
            if batch.is_last_batch():
                self._produce_queue.send(batch.encode())
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            accumulator = {}
            for row in batch.iter_per_header():
                try:
                    bucket = self._bucket_fn(row[self._time_col])
                    key = row[self._key_col]
                    value = float(row[self._value_col])
                    acc_key = (bucket, key)
                    accumulator[acc_key] = accumulator.get(acc_key, 0.0) + value
                except Exception:
                    logging.error(f"[aggregator] row malformed: {row}")
                    continue
            self._flush(batch, accumulator)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logging.exception("[aggregator] Error procesando batch")
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                logging.exception("[aggregator] Error al hacer basic_nack")

    def _flush(self, batch, accumulator):
        header = [self._bucket_name, self._key_col, self._out_value_name]
        rows = [[bucket, str(key), f"{total:.2f}"] for (bucket, key), total in accumulator.items()]
        
        batch.delete_rows()
        batch.set_header(header)
        batch.add_rows(rows)
        self._produce_queue.send(batch.encode())