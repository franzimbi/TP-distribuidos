from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareExchange
from common.batch import Batch
import logging
import signal
import sys
import threading
import time
# from diskcache import Cache

# cache_dir = '/tmp/join_cache'
# cache_size =10*(2 ** 30)  # 10GB

# FLUSH_MESSAGE = 'FLUSH'
# END_MESSAGE = 'END'

class Join:
    def __init__(self, confirmation_queue, join_queue, column_id, column_name, is_last_join):
        self.producer_queue = None
        self.consumer_queue = None
        self.join_dictionary = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = join_queue
        self.thead_join = None
        self.confirmation_queue = confirmation_queue
        self.is_last_join = is_last_join
        self.lock = threading.Lock()

        signal.signal(signal.SIGTERM, self.graceful_quit)
        signal.signal(signal.SIGINT, self.graceful_quit)

        # recibe de join_queue los datos y arma el diccionario de id-valor

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
        print("entre a start")
        aux = self.join_queue

        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=aux)
        name_q_confirm = self.confirmation_queue
        self.confirmation_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=name_q_confirm)

        # self.thead_join = threading.Thread(target=self.join_queue.start_consuming, args=(self.callback_to_receive_join_data,), daemon=True)
        # self.thead_join.start()
        self.join_queue.start_consuming(self.callback_to_receive_join_data)        

        print("\ntermine el  join")

        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)

        print("\narranque consumer join")
        self.consumer_queue.start_consuming(self.callback)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()

        print(f"\n\n[JOINER] Recibi batch {batch.id()} de client_{client_id} con {batch.size()} filas")

        if batch.is_last_batch():
            print(f"\n\n[JOINER] llego last batch de client_{client_id}, envio confirmacion al distributor.\n me quedo con {self.join_dictionary[client_id]}")
            self.confirmation_queue.send(batch.encode())
            self.join_queue.stop_consuming()
            return

        id = batch.index_of(self.column_id)
        name = batch.index_of(self.column_name)
        
        if id is None or name is None:
            print("hola")
            logging.debug(f"Column {self.column_id} or {self.column_name} not found in batch header {batch.get_header()}")
            return
        
        for row in batch:
            key = row[id]
            value = row[name]
            if key is None or value is None:
                logging.debug(f"Key or value is None for row: {row}")
                continue
            with self.lock:
                if client_id not in self.join_dictionary:
                    print(f"[JOINER] Nuevo client_id {client_id} en el joiner {self.column_name}")
                    self.join_dictionary[client_id] = {}
                if key not in self.join_dictionary[client_id]:
                    self.join_dictionary[client_id][key] = value


    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()
        logging.debug(f"[JOIN] Procesando batch {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
        
        if batch.is_last_batch():
            self.producer_queue.send(batch.encode())
            return
        try:
            batch.change_header_name_value(self.column_id, self.column_name, self.join_dictionary[client_id], self.is_last_join)
        except (ValueError, KeyError) as e:
            logging.error(
                f'action: join_batch_with_dicctionary | result: fail | error: {e}')
        self.producer_queue.send(batch.encode())

    def close(self):
        # Intentamos parar y cerrar todos los queues
        try:
            if self.consumer_queue:
                try:
                    self.consumer_queue.stop_consuming()
                except Exception:
                    pass
                try:
                    self.consumer_queue.close()
                except Exception:
                    pass

            if self.producer_queue:
                try:
                    self.producer_queue.close()
                except Exception:
                    pass

            if self.join_queue:
                try:
                    self.join_queue.stop_consuming()
                except Exception:
                    pass
                try:
                    self.join_queue.close()
                except Exception:
                    pass
                try:
                    self.thead_join.join()
                except Exception:
                    pass
        except Exception:
            pass