from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from common.batch import Batch
import logging
import signal
import sys
import threading
from diskcache import Cache

cache_dir = '/tmp/join_cache'
cache_size = 2 ** 30  # 1GB

FLUSH_MESSAGE = 'FLUSH'
END_MESSAGE = 'END'


class Join:
    def __init__(self,join_queue, column_id, column_name, use_diskcache=False):
        self.producer_queue = None
        self.consumer_queue = None
        self.join_dictionary = None
        self.coordinator_consumer = None
        self.coordinator_producer = None
        self.conection_coordinator = None
        if use_diskcache:
            self.join_dictionary = Cache(cache_dir, size_limit=cache_size)
        else:
            self.join_dictionary = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = join_queue
        self.lock = threading.Lock()

        signal.signal(signal.SIGTERM, self.graceful_quit)

        # recibe de join_queue los datos y arma el diccionario de id-valor

    def graceful_quit(self, signum, frame):
        try:
            print("Recibida señal SIGTERM, cerrando joiner...")
            self.close()
            print("Joiner cerrado correctamente.")
        except Exception as e:
            logging.error(f"Error cerrando joiner: {e}")
        sys.exit(0)

    def start(self, consumer, producer, coordinator_consumer, coordinator_producer):
        aux = self.join_queue
        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=aux)
        self.join_queue.start_consuming(self.callback_to_receive_join_data)
        logging.debug("action: receive_join_data | status: finished | entries: %d",
                      len(self.join_dictionary))
        # self.join_queue.close()


        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)
        self.coordinator_consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_consumer)
        self.coordinator_producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_producer)

        self.conection_coordinator = threading.Thread(
            target=self.coordinator_consumer.start_consuming,
            args=(self.coordinator_callback,), daemon=True
        )
        self.conection_coordinator.start()

        self.consumer_queue.start_consuming(self.callback)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        id = batch.index_of(self.column_id)
        name = batch.index_of(self.column_name)

        if id is None or name is None:
            logging.error(f"Column {self.column_id} or {self.column_name} not found in batch header")
            return
        for row in batch:
            key = row[id]
            value = row[name]
            if key is None or value is None:
                logging.error(f"Key or value is None for row: {row}")
                continue
            if key not in self.join_dictionary:
                self.join_dictionary[key] = value

        if batch.is_last_batch():
            self.join_queue.stop_consuming()


    def coordinator_callback(self, ch, method, properties, body):
        msg = body.decode('utf-8')
        if str(msg) == str(FLUSH_MESSAGE):
            with self.lock:
                print(f"[JOIN] Recibido comando FLUSH del coordinator. Enviando datos al siguiente nodo.")
                self.coordinator_producer.send(END_MESSAGE)
        else:
            logging.error(f"[FILTER] Unknown command from coordinator: {msg}")

    def callback(self, ch, method, properties, message):
        with self.lock:
            batch = Batch()
            batch.decode(message)
            print(f"[JOIN] Procesando batch {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
            try:
                batch.change_header_name_value(self.column_id, self.column_name, self.join_dictionary)
            except (ValueError, KeyError) as e:
                logging.error(
                    f'action: join_batch_with_dicctionary | result: fail | error: {e}')
            if batch.is_last_batch():
                print(f"[JOIN] Recibido batch final {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
                self.coordinator_producer.send(batch.encode())
                return
            self.producer_queue.send(batch.encode())

    def close(self):
        try:
            if self.consumer_queue:
                self.consumer_queue.stop_consuming()
                self.consumer_queue.close()
            if self.producer_queue:
                self.producer_queue.close()
            if self.join_queue:
                self.join_queue.close()
            if self.coordinator_consumer:
                self.coordinator_consumer.stop_consuming()
                self.coordinator_consumer.close()
            if self.coordinator_producer:
                self.coordinator_producer.close()
        except:
            pass
        if self.conection_coordinator:
            self.conection_coordinator.join()
        if isinstance(self.join_dictionary, Cache):
            self.join_dictionary.close()
