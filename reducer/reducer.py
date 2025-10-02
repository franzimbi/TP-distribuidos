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
        self.top_users = {} # key: store_id, value: dict of user_id -> purchases_qty
        self._columns = [n.strip() for n in columns.split(",")] #store_id, user_id,purchases_qty

        logging.debug(f"[REDUCER] Initialized with top={self._top} and columns={self._columns}")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        try:
            print("Recibida señal SIGTERM, cerrando reducer...")
            self.close()
            print("[REDUCER] Apagado limpio.")
        except Exception as e:
            logging.error(f"[REDUCER] Error closing: {e}")
        sys.exit(0)

    def start(self):
        self._consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)
        logging.debug(f"[REDUCER] Recibido batch {batch.id()} de tipo {batch.type()} con {len(batch)} filas.")
        for i in batch.iter_per_header():
            store = i[self._columns[0]]
            user = i[self._columns[1]]
            qty = int(float(i[self._columns[2]]))
            if store not in self.top_users:
                self.top_users[store] = {}

            self.top_users[store][user] = qty
            top_keys = heapq.nlargest(self._top, self.top_users[store], key=self.top_users[store].get) # se queda con el top 3 mas grande en orden
            self.top_users[store] = {k: self.top_users[store][k] for k in top_keys}

        if batch.is_last_batch():
            print(f"[REDUCER] Recibido batch final {batch.id()} de tipo {batch.type()}. Procesando resultados...")
            try:
                rows = []
                for store, users in self.top_users.items():
                    for user, qty in users.items():
                        rows.append((store, user))
                logging.debug(f"[REDUCER] Processed rows: {rows}")
                rows = [(str(int(float(s))), str(int(float(u)))) for (s, u) in rows]
                rows.sort(key=lambda x: (int(x[0]), int(x[1])))
                self.send_last_batch(batch, rows)   
            except Exception as e:
                logging.error(f"[REDUCER] Error sending last batch: {e}")

    def send_last_batch(self, batch, rows):
        batch_result = Batch(batch.id(), batch.get_query_id(), type_file=batch.type())
        batch_result.set_header([self._columns[0], self._columns[1]])
        batch_result.set_last_batch()
        for store, user in rows:
            batch_result.add_row([store, user])
        self._producer_queue.send(batch_result.encode())

    def close(self):
        self._consumer_queue.stop_consuming()
        self._consumer_queue.close()
        self._producer_queue.close()
        print("Queues cerradas")