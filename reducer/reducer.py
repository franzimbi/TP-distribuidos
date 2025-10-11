from common.batch import Batch
import logging
import heapq
import signal
import sys


class Reducer:
    def __init__(self, consumer, producer, top, columns):
        self._consumer_queue = consumer
        self._producer_queue = producer
        self._top = int(top)
        self.top_users = {}  # key: store_id, value: dict of user_id -> purchases_qty
        self.counter_batches = {}
        self.waited_batches = {}
        self._columns = [n.strip() for n in columns.split(",")]  # store_id, user_id,purchases_qty

        logging.debug(f"[REDUCER] Initialized with top={self._top} and columns={self._columns}")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        try:
            logging.debug("[REDUCER] Recibida señal SIGTERM, cerrando reducer...")
            self.close()
            logging.debug("[REDUCER] Apagado limpio.")
        except Exception as e:
            logging.error(f"[REDUCER] Error closing: {e}")
        sys.exit(0)

    def start(self):
        self._consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()
        if client_id not in self.counter_batches:
            self.counter_batches[client_id] = 0
            self.waited_batches[client_id] = None

        if batch.is_last_batch():
            self.waited_batches[client_id] = int(batch[0][batch.get_header().index('cant_batches')])
            if self.waited_batches[client_id] == self.counter_batches[client_id]:  # llegaron todos
                self.send_last_batch(batch, client_id)
                return

        for i in batch.iter_per_header():
            store = i[self._columns[0]]
            user = i[self._columns[1]]
            qty = int(float(i[self._columns[2]]))
            if client_id not in self.top_users:
                self.top_users[client_id] = {}
            if store not in self.top_users[client_id]:
                self.top_users[client_id][store] = {}

            self.top_users[client_id][store][user] = qty
            top_keys = heapq.nlargest(self._top, self.top_users[client_id][store], key=self.top_users[client_id][
                store].get)  # se queda con el top n mas grande en orden
            self.top_users[client_id][store] = {k: self.top_users[client_id][store][k] for k in top_keys}
        self.counter_batches[client_id] += 1
        if self.waited_batches[client_id] is not None and self.counter_batches[client_id] == self.waited_batches[
            client_id]:
            self.send_last_batch(batch, client_id)

    def send_last_batch(self, batch, client_id):
        rows = []
        try:
            for store, users in self.top_users[client_id].items():
                for user, qty in users.items():
                    rows.append((store, user, qty))
            rows = [(str(int(float(s))), str(int(float(u))), str(float(v))) for (s, u, v) in rows]
            rows.sort(key=lambda x: (int(x[0]), int(x[1])))
        except Exception as e:
            logging.error(f"[REDUCER] Error sending last batch: {e}")

        batch_result = Batch(batch.id() - 1, client_id=client_id, type_file=batch.type())
        batch_result.set_header([self._columns[0], self._columns[1], self._columns[2]])
        for store, user, qty in rows:
            batch_result.add_row([store, user, str(qty)])
        batch_result.set_client_id(client_id)
        self._producer_queue.send(batch_result.encode())

        batch.delete_rows()
        batch.set_last_batch(True)
        batch.set_header(['cant_batches'])
        batch.add_row([str(1)])
        self._producer_queue.send(batch.encode())
        self.top_users.pop(client_id)
        self.waited_batches.pop(client_id)
        self.counter_batches.pop(client_id)

    def close(self):
        self._consumer_queue.stop_consuming()
        self._consumer_queue.close()
        self._producer_queue.close()
        logging.debug("[REDUCER] Queues cerradas")
