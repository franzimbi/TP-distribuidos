from common.batch import Batch
import logging
import signal
import sys
from middleware.middleware import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUFFER_SIZE = 150
class Counter:
    def __init__(self, consumer, producer, *, key_columns, count_name):
        self._consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self._producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)
        self._accumulator = {}  # key: (store_id, user_id), value: count

        self.key_columns = key_columns
        self.count_name = count_name
        self.qid = None

        signal.signal(signal.SIGTERM, self.graceful_quit)
    
    def graceful_quit(self, signum, frame):
        logging.debug("Recibida señal SIGTERM, cerrando counter...")
        self.stop()
        logging.debug("Counter cerrado correctamente.")
        sys.exit(0)

    def start(self):
        self._consumer_queue.start_consuming(self.callback)


    def stop(self):

        self._consumer_queue.stop_consuming()
        self._consumer_queue.close()
        self._producer_queue.close()
        

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)

        if self.qid is None:
            self.qid = batch.get_query_id()

        for row in batch.iter_per_header():
            try:
                key = tuple(str(row[col]).strip() for col in self.key_columns)
                if any(not k for k in key):
                    continue
                self._accumulator[key] = self._accumulator.get(key, 0) + 1
            except Exception as e:
                logger.warning(f"[COUNTER] Malformed row: {row} | Error: {e}")
                continue

        if batch.is_last_batch():
            self.flush(batch)
            return

    def flush(self, src_batch):
        if not self._accumulator:
            logging.error("[COUNTER] No data to flush")
            return

        header = self.key_columns + [self.count_name]
        rows = [[*key, str(cnt)] for key, cnt in self._accumulator.items()]

        for i in range(0, len(rows), BUFFER_SIZE):
            chunk = rows[i:i + BUFFER_SIZE]
            last_chunk = i + BUFFER_SIZE >= len(rows)
            out_batch = Batch(
                id=src_batch.id(),
                query_id=self.qid,
                last=last_chunk,
                type_file=src_batch.type(),
                header=header,
                rows=chunk
            )
            self._producer_queue.send(out_batch.encode())

        self._accumulator.clear()
        self.qid = None

