from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareExchange
from common.batch import Batch
import logging
import signal
import sys
import threading

class Join:
    def __init__(self, confirmation_queue, join_queue, column_id, column_name, is_last_join):
        self.producer_queue = None
        self.consumer_queue = None
        self.confirmation_queue = confirmation_queue
        self.join_dictionary = None
        self.is_last_join = is_last_join
        self.batch_counter = 0
        self.join_dictionary = {}
        self.counter_batches = {}
        self.waited_batches = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = join_queue
        self.thead_join = None
        self.lock = threading.Lock()

        signal.signal(signal.SIGTERM, self.graceful_quit)
        signal.signal(signal.SIGINT, self.graceful_quit)

    def graceful_quit(self, signum, frame):
        try:
            logging.debug("Recibida señal SIGTERM, cerrando joiner...")
            self.close()
            logging.debug("Joiner cerrado correctamente.")
        except Exception as e:
            sys.exit(0)
            pass
        sys.exit(0)

    def start(self, consumer, producer):
        aux = self.join_queue
        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=aux)
        queue_confirm_name = self.confirmation_queue
        self.confirmation_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_confirm_name)

        self.thead_join = threading.Thread(target=self.join_queue.start_consuming,
                                           args=(self.callback_to_receive_join_data,), daemon=True)
        self.thead_join.start()
        logging.debug("action: receive_join_data | status: finished | entries: %d",
                      len(self.join_dictionary))

        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)

        self.consumer_queue.start_consuming(self.callback)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        self.batch_counter += 1
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()

        if client_id not in self.counter_batches:
            self.counter_batches[client_id] = 0
            self.waited_batches[client_id] = None

        id = batch.index_of(self.column_id)
        name = batch.index_of(self.column_name)

        if batch.is_last_batch():
            self.confirmation_queue.send(batch.encode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if id is None or name is None:
            logging.debug(
                f"Column {self.column_id} or {self.column_name} not found in batch header {batch.get_header()}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        for row in batch:
            key = row[id]
            value = row[name]
            if key is None or value is None:
                logging.debug(f"Key or value is None for row: {row}")
                continue
            with self.lock:
                if client_id not in self.join_dictionary:
                    self.join_dictionary[client_id] = {}
                if key not in self.join_dictionary[client_id]:
                    self.join_dictionary[client_id][key] = value
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()

        if batch.is_last_batch():
            self.waited_batches[client_id] = int(batch[0][batch.get_header().index('cant_batches')])
            if self.waited_batches[client_id] == self.counter_batches[client_id]:  # llegaron todos
                self.join_dictionary.pop(client_id, None)
            self.producer_queue.send(batch.encode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            with self.lock:
                batch.change_header_name_value(self.column_id, self.column_name, self.join_dictionary[client_id],
                                               self.is_last_join)
        except (ValueError, KeyError) as e:
            logging.error(
                f'action: join_batch_with_dicctionary[{client_id}] | result: fail | error: {e}')
        self.counter_batches[client_id] += 1
        if self.waited_batches[client_id] is not None and self.counter_batches[client_id] == self.waited_batches[
            client_id]:
            self.join_dictionary.pop(client_id, None)
        self.producer_queue.send(batch.encode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def close(self):
        try:
            if self.consumer_queue:
                self.consumer_queue.stop_consuming()
                self.consumer_queue.close()


            if self.producer_queue:
                self.producer_queue.close()


            if self.join_queue:
                self.join_queue.stop_consuming()
                self.join_queue.close()

            if self.thead_join is not None:
                self.thead_join.join()
        except Exception:
            pass
