import logging
import signal
import sys
from datetime import datetime
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])

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

class Aggregator:
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

        kind = bucket_kind.lower().strip()
        self._bucket_fn = BUCKET[kind]
        self._bucket_name = bucket_name.strip()

        self.accumulator = {}
        self._qid = None

        signal.signal(signal.SIGTERM, self.graceful_quit)

    def graceful_quit(self, signum, frame):
        logging.debug("Recibida señal SIGTERM, cerrando aggregator...")
        self.stop()
        logging.debug("Aggregator cerrado correctamente.")
        sys.exit(0)

    def start(self):
        self._consume_queue.start_consuming(self.callback)

    def stop(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        self._produce_queue.close()
        

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)
        # if batch.id() % 50000 == 0 or batch.id() == 0:
        #     print(f"[AGGREGATOR] Procesando batch {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
        # if batch.is_last_batch():
        #     print(f"[AGGREGATOR] Recibido batch final {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
        if self._qid is None:
            self._qid = batch.get_query_id()

        for row in batch.iter_per_header():
            try:
                bucket = self._bucket_fn(row[self._time_col])
                key = row[self._key_col]
                value = float(row[self._value_col])
                acc_key = (bucket, key)
                self.accumulator[acc_key] = self.accumulator.get(acc_key, 0.0) + value
            except Exception:
                logging.error(f"[aggregator] row malformed: {row}")
                continue
                
        if batch.is_last_batch():
            self._flush(batch)
            return

    def _flush(self, src_batch):
        if not self.accumulator:
            return

        header = [self._bucket_name, self._key_col, self._out_value_name]
        rows = [[bucket, str(key), f"{total:.2f}"] for (bucket, key), total in self.accumulator.items()]
       
        for i in range(0, len(rows), BUFFER_SIZE):
            chunk = rows[i:i + BUFFER_SIZE]
            is_last_chunk = i + BUFFER_SIZE >= len(rows)
            out_batch = Batch(
                id=src_batch.id(),
                query_id=self._qid,
                last=is_last_chunk,
                type_file=src_batch.type(),
                header=header,
                rows=chunk
            )
            out_batch.set_client_id(src_batch.client_id())
            self._produce_queue.send(out_batch.encode())

        self.accumulator.clear()
        self._qid = None