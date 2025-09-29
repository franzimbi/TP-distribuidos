from common.batch import Batch
import logging
import heapq

class Reducer:
    def __init__(self, consumer, producer, top, columns):
        self._consumer_queue = consumer
        self._producer_queue = producer
        self._top = int(top)
        self.top_users = {}
        self._columns = [n.strip() for n in columns.split(",")] #store_id, user_id
        self.bol = True

        logging.info(f"[REDUCER] Initialized with top={self._top} and columns={self._columns}")

    def start(self):
        logging.info("[REDUCER] Starting consumer...")
        self._consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch(); batch.decode(message)

        if self.bol:
            logging.info(f"\n\n\n[REDUCER] Received batch {batch.id()} {batch}\n\n")
            self.bol = False
        if batch.id() == 0 or batch.is_last_batch():
            logging.info(f"[REDUCER] Received batch {batch.id()} {batch}")
        for i in batch.iter_per_header():
            store = i["store_id"]
            user = i["user_id"]
            qty = int(i["purchases_qty"])
            if store not in self.top_users:
                self.top_users[store] = {}

            self.top_users[store][user] = qty
            top_keys = heapq.nlargest(self._top, self.top_users[store], key=self.top_users[store].get) # se queda con el top 3 mas grande en orden
            self.top_users[store] = {k: self.top_users[store][k] for k in top_keys}

        if batch.is_last_batch():
            logging.info(f'\n\n[REDUCER] top_users: {self.top_users}')
            batch_result = Batch(batch.id(), batch.get_query_id(), type_file=batch.type())
            batch_result.set_header(['store_id', 'user_id', 'purchases_qty'])
            batch_result.set_last_batch()
            for store in self.top_users:
                for user in store:
                    logging.info(f"[REDUCER] Adding row: store={store}, user={user}, purchases_qty={self.top_users[store][user]}")
                    batch_result.add_row([store, user, self.top_users[store][user]])
            logging.info(f"[REDUCER] Sending batch {batch_result.id()} with {len(batch_result)} rows.")
            self._producer_queue.send(batch_result.encode())


