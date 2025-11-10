import logging
import signal
import sys
from datetime import datetime
from collections import defaultdict
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
from common.id_range_counter import IDRangeCounter
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])

class Accumulator:
    def __init__(self, consume_queue, produce_queue, *,
                 key_col: str,
                 value_col: str,
                 bucket_col: str,
                 out_value_name: str):
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)

        self._key_col = key_col
        self._value_col = value_col
        self.bucket_col = bucket_col
        self._out_value_name = out_value_name

        self.client_state = defaultdict(lambda: {
            "accumulator": {},
            "expected": None,
            "received": 0,
            "type": None,
            "id_counter": IDRangeCounter(),
        })
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
        cid = batch.client_id()
        state = self.client_state[cid]

        if state["id_counter"].already_processed(batch.id(), ' '):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if batch.is_last_batch():
            try:
                idx = batch.get_header().index('cant_batches')
                state["expected"] = int(batch[0][idx])
                logging.debug(f"[aggregator] cid={cid} espera {state['expected']} batches")
            except (ValueError, IndexError):
                logging.warning(f"[aggregator] cid={cid} last_batch sin 'cant_batches' válido")

            if state["expected"] is not None and state["received"] == state["expected"]:
                logging.debug(f"[aggregator] flusheo pq state[received] == state[expected]")
                self._flush_client(cid)
            state["id_counter"].add_id(batch.id(), ' ')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        state["type"] = batch.type()
        for row in batch.iter_per_header():
            try:
                bucket = row[self.bucket_col]
                key = row[self._key_col]
                value = float(row[self._value_col])
                acc_key = (bucket, key)
                state["accumulator"][acc_key] = state["accumulator"].get(acc_key, 0.0) + value
            except Exception:
                logging.error(f"[aggregator] cid={cid} row malformed: {row}")
                continue

        state["received"] += 1
        logging.debug(f"[aggregator] cid={cid} received={state['received']}")

        if state["expected"] is not None and state["received"] == state["expected"]:
            logging.debug(f"[aggregator]llegue a la cantidad esperada")
            self._flush_client(cid)

        state["id_counter"].add_id(batch.id(), ' ')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _flush_client(self, cid: int):
        state = self.client_state[cid]
        if not state["accumulator"]:
            state["expected"] = None
            state["received"] = 0
            state["type"] = None
            return

        header = [self.bucket_col, self._key_col, self._out_value_name]
        rows = [[bucket, str(key), f"{total:.2f}"] for (bucket, key), total in state["accumulator"].items()]

        total_rows = len(rows)
        count_batches = 0
        for i in range(0, total_rows, BUFFER_SIZE): 
            chunk = rows[i:i + BUFFER_SIZE]
            out_batch = Batch(
                id=count_batches,
                query_id=0,
                client_id=cid,
                last=False,
                type_file=state["type"],
                header=header,
                rows=chunk
            )
            self._produce_queue.send(out_batch.encode())
            count_batches += 1

        last_batch = Batch(
            id=count_batches,
            query_id=0,
            client_id=cid,
            last=True,
            type_file=state["type"],
            header=['cant_batches'],
            rows=[[str(count_batches)]],
        )
        self._produce_queue.send(last_batch.encode())

        state["accumulator"].clear() #TODO: borrar los states
        state["expected"] = None
        state["received"] = 0
        state["type"] = None
        state["id_counter"] = IDRangeCounter()
