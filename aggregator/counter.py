from common.batch import Batch
import logging
from middleware.middleware import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUFFER_SIZE = 150

class Counter:
    def __init__(self, consumer, producer):
        self._consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self._producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)
        self._accumulator = {} # key: (store_id, user_id), value: count

    def start(self):
        self._consumer_queue.start_consuming(self.callback)

    def stop(self):
        try: self._consumer_queue.stop_consuming()
        except Exception: pass
        try: self._consumer_queue.close()
        except Exception: pass
        try: self._producer_queue.close()
        except Exception: pass

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)

        if batch.is_last_batch():
            self.flush(batch)
            end_batch = Batch(
                id=batch.id(),
                query_id=batch.get_query_id(),
                last=True,
                type_file=batch.type(),
                header=[],
                rows=[]
            )
            self._accumulator.clear()
            self._producer_queue.send(end_batch.encode())
            logger.info("[COUNTER] END sent")
            return
        
        for row in batch.iter_per_header():
            try:
                store_id = row["store_id"]
                user_id = row["user_id"]

                key = (store_id, user_id)
                self._accumulator[key] = self._accumulator.get(key, 0) + 1
            except Exception as e:
                logger.error(f"[COUNTER] Malformed row: {row} | Error: {e}")
                continue

    def flush(self, src_batch):
        if not self._accumulator:
            logging.error("[COUNTER] No data to flush")
            return
        
        header = ["store_id", "user_id", "purchases_qty"]
        rows = [[store_id, user_id, str(count)] for (store_id, user_id), count in self._accumulator.items()]

        for i in range(0, len(rows), BUFFER_SIZE):
            chunk = rows[i:i + BUFFER_SIZE]
            out_batch = Batch(
                id=src_batch.id(),
                query_id=src_batch.get_query_id(),
                last=False,
                type_file=src_batch.type(),
                header=header,
                rows=chunk
            )   
            if i == 150:
                logger.info(f"[COUNTER] Sending batch {out_batch}")
            self._producer_queue.send(out_batch.encode())
            # logger.info(f"[COUNTER] Sent batch {out_batch.id()}")
        

# [
#   ["store_id", "user_id", "purchases_qty"],
#   ['1', 'u_01', '65'],
#   ['1', 'u_21', '42'],
#   ['2', 'u_09', '81']
# ]
