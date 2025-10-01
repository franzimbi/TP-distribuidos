import logging
from datetime import datetime
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch

BUFFER_SIZE = 150
OUT_HEADER = ["year_semester", "store_id", "tpv"]


class TPVBySemesterStore:
    def __init__(self, consume_queue, produce_queue):
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)
        self._acc = {}
        self._qid = None

    def start(self):
        self._consume_queue.start_consuming(self.callback)

    def stop(self):
        try:
            self._consume_queue.stop_consuming()
        except Exception:
            pass
        try:
            self._consume_queue.close()
        except Exception:
            pass
        try:
            self._produce_queue.close()
        except Exception:
            pass

    def _year_semester(self, created_at_str: str) -> str:
        dt = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
        return f"{dt.year}_{'S1' if dt.month <= 6 else 'S2'}"

    def callback(self, ch, method, properties, message):
        batch = Batch();
        batch.decode(message)

        if self._qid is None:
            self._qid = batch.get_query_id()

        for row in batch.iter_per_header():
            try:
                year_sem = self._year_semester(row["created_at"])
                store_id = row["store_id"]
                amount = float(row["final_amount"])
                key = (year_sem, store_id)
                self._acc[key] = self._acc.get(key, 0.0) + amount
            except Exception:
                logging.error(f"[aggregator] fila mal formada: {row}")
                continue

        if batch.is_last_batch():
            self._flush(batch)
            return

    def _flush(self, src_batch):
        if not self._acc:
            return

        rows = [[ys, str(sid), f"{tpv:.2f}"] for (ys, sid), tpv in self._acc.items()]
        rows.sort(key=lambda r: (r[0], int(r[1])) if r[1].isdigit() else (r[0], r[1]))

        qid = (self._qid or 0)
        out_id = src_batch.id() if src_batch else 0

        total_rows = len(rows)
        sent_batches = 0

        for i in range(0, total_rows, BUFFER_SIZE):
            chunk = rows[i:i + BUFFER_SIZE]
            is_last_chunk = i + BUFFER_SIZE >= total_rows
            outb = Batch(
                id=out_id,
                query_id=qid,
                last=is_last_chunk,
                type_file=src_batch.type(),
                header=list(OUT_HEADER),
                rows=chunk
            )
            self._produce_queue.send(outb.encode())
            sent_batches += 1
        self._acc.clear()
