from common.batch import Batch
import logging
from middleware.middleware import MessageMiddlewareQueue
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUFFER_SIZE = 150


class Counter:
    def __init__(self, consumer, producer):
        self._consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self._producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)
        self._accumulator = {}  # key: (store_id, user_id), value: count

    def start(self):
        self._consumer_queue.start_consuming(self.callback)

    def stop(self):
        try:
            self._consumer_queue.stop_consuming()
        except Exception:
            pass
        try:
            self._consumer_queue.close()
        except Exception:
            pass
        try:
            self._producer_queue.close()
        except Exception:
            pass

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)

        for row in batch.iter_per_header():
            store_id = row.get("store_id")
            user_id_raw = row.get("user_id")
            created_at = row.get("created_at")
            if not created_at:
                continue

            if not store_id or not user_id_raw:
                continue
            store_id = str(store_id).strip()
            try:
                year = int(created_at[:4])
                if year not in (2024, 2025):
                    continue
                user_id = str(int(float(user_id_raw)))
                key = (store_id, user_id)
                self._accumulator[key] = self._accumulator.get(key, 0) + 1
            except ValueError:
                logger.warning(f"[COUNTER] Invalid user_id: {user_id_raw}")
                continue
            except Exception as e:
                logger.error(f"[COUNTER] Malformed row: {row} | Error: {e}")
                continue
            
        if batch.is_last_batch():
            self.flush(batch)
            self._accumulator.clear()
            logger.info("[COUNTER] END sent")
            return

    def flush(self, src_batch):
        if not self._accumulator:
            logging.error("[COUNTER] No data to flush")
            return

        header = ["store_id", "user_id", "purchases_qty"]
        rows = [[store_id, user_id, str(count)] for (store_id, user_id), count in self._accumulator.items()]

        for i in range(0, len(rows), BUFFER_SIZE):
            chunk = rows[i:i + BUFFER_SIZE]
            last_chunk = i + BUFFER_SIZE >= len(rows)
            out_batch = Batch(
                id=src_batch.id(),
                query_id=src_batch.get_query_id(),
                last=last_chunk,
                type_file=src_batch.type(),
                header=header,
                rows=chunk
            )
            self._producer_queue.send(out_batch.encode())

