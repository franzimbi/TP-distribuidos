from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from common.batch import Batch
import logging
import signal
import sys
from diskcache import Cache

cache_dir = '/tmp/join_cache'
cache_size = 2 ** 30  # 1GB


class Join:
    def __init__(self, join_queue, column_id, column_name, use_diskcache=False):
        self.producer_queue = None
        self.consumer_queue = None
        self.join_dictionary = None
        if use_diskcache:
            self.join_dictionary = Cache(cache_dir, size_limit=cache_size)
        else:
            self.join_dictionary = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=join_queue)

        signal.signal(signal.SIGTERM, self.graceful_quit)

        # recibe de join_queue los datos y arma el diccionario de id-valor
        logging.info("action: receive_join_data | status: waiting")
        self.join_queue.start_consuming(self.callback_to_receive_join_data)
        logging.info("action: receive_join_data | status: finished | entries: %d | diskcache: {%s}",
                     len(self.join_dictionary), str(use_diskcache))

    def graceful_quit(self, signum, frame):
        logging.info("SIGTERM recibido, cerrando joiner...")
        try:
            self.close()
        except Exception as e:
            logging.error(f"Error cerrando joiner: {e}")
        try:
            if self.consumer_queue:
                self.consumer_queue.close()
            if self.producer_queue:
                self.producer_queue.close()
            if self.join_queue:
                self.join_queue.close()
        except:
            pass
        sys.exit(0)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        # logging.info("action: receive_join_data | batch: %s", batch)
        id = batch.index_of(self.column_id)
        name = batch.index_of(self.column_name)

        if id is None or name is None:
            logging.debug(f"Column {self.column_id} or {self.column_name} not found in batch header")
            return
        for row in batch:
            key = row[id]
            value = row[name]
            if key is None or value is None:
                logging.debug(f"Key or value is None for row: {row}")
                continue
            if key not in self.join_dictionary:
                self.join_dictionary[key] = value

        if batch.is_last_batch():
            self.join_queue.stop_consuming()

    def start(self, consumer, producer):
        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)
        self.consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        # if self.query4:
        #     logging.info(f'[QUERY 4 callback] receive batch: {batch}')
        try:
            batch.change_header_name_value(self.column_id, self.column_name, self.join_dictionary)
            logging.debug(f'action: join_batch_with_dicctionary | result: success')
        except (ValueError, KeyError) as e:
            logging.error(
                f'action: join_batch_with_dicctionary | result: fail | errror: {e} | dic: {self.join_dictionary}')
        self.producer_queue.send(batch.encode())

    def close(self):
        if isinstance(self.join_dictionary, Cache):
            self.join_dictionary.close()
