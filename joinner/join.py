from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareExchange
from common.batch import Batch
import logging
import signal
import sys
import threading
import time
from diskcache import Cache

cache_dir = '/tmp/join_cache'
cache_size =10*(2 ** 30)  # 10GB

FLUSH_MESSAGE = 'FLUSH'
END_MESSAGE = 'END'

class Join:
    def __init__(self, exchange_name, join_queue, column_id, column_name, use_diskcache=False):
        self.producer_queue = None
        self.consumer_queue = None
        self.join_dictionary = None
        self.use_diskcache = use_diskcache
        self.batch_counter = 0
        self.disk_buffer = {}
        if use_diskcache:
            self.join_dictionary = Cache(cache_dir, size_limit=cache_size)
        else:
            self.join_dictionary = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = join_queue
        self.exchange_name = exchange_name
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

        print(f"[JOINER] Me BINDEO al exchange {self.exchange_name} y a la cola {aux}")
        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=aux)
        # self.join_queue = MessageMiddlewareExchange(
        #     host='rabbitmq', exchange_name=self.exchange_name,
        #     route_keys=[''], exchange_type='fanout', queue_name=aux
        # )

        print(f"[JOINER] Soy el joiner {self.column_name} y tengo que armar el diccionario")

        self.join_queue.start_consuming(self.callback_to_receive_join_data)

        print(f"[JOINER] Soy el joiner {self.column_name} y ya arme el diccionario, ahora tengo que esperar a que me digan FLUSH")
        logging.debug("action: receive_join_data | status: finished | entries: %d",
                      len(self.join_dictionary))
        # self.join_queue.close()

        print("termine de recibir los datos del join")
        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)

        self.consumer_queue.start_consuming(self.callback)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        self.batch_counter += 1
        batch = Batch()
        batch.decode(message)

        header = batch.get_header()
        # if batch.id() == 0 or batch.id() % 5000 == 0:
        print(f"[JOINER] batch.id={batch.id()} | last={batch.is_last_batch()} | header={header}")

        id = batch.index_of(self.column_id)
        name = batch.index_of(self.column_name)
        
        if self.use_diskcache and batch.is_last_batch():
            print(f"[JOINER] Flusheando buffer ({len(self.disk_buffer)} claves) a disco...")
            self._flush_buffer_to_disk()
            self.batch_counter = 0

        if batch.is_last_batch():
            print(f"[JOINER] LAST batch recibido (id={batch.id()}) -> llamo a stop_consuming()")
            try:
                self.join_queue.stop_consuming()
                print("[JOINER] stop_consuming() OK")
            except Exception as e:
                print(f"[JOINER] stop_consuming() lanzó excepción: {e}")

        if id is None or name is None:
            logging.debug(f"Column {self.column_id} or {self.column_name} not found in batch header {header}")
            return
        
        added = 0
        total_rows = 0
        for row in batch:
            total_rows += 1
            key = row[id]
            value = row[name]
            if key is None or value is None:
                logging.debug(f"Key or value is None for row: {row}")
                continue
            if not self.use_diskcache and key not in self.join_dictionary:
                self.join_dictionary[key] = value
                added += 1
            elif self.use_diskcache and key not in self.join_dictionary and key not in self.disk_buffer:
                self.disk_buffer[key] = value
                added += 1

    def _flush_buffer_to_disk(self):
        try:
            with self.join_dictionary.transact():
                for k, v in self.disk_buffer.items():
                    self.join_dictionary[k] = v
            self.disk_buffer.clear()
            self.batch_counter = 0
            print(f"[JOINER] Flush completado correctamente ({len(self.join_dictionary)} total)")
        except Exception as e:
            print(f"[JOINER] Error durante el flush a disco: {e}")


    def callback(self, ch, method, properties, message):
        with self.lock:
            batch = Batch()
            batch.decode(message)
            logging.debug(f"[JOIN] Procesando batch {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
            if batch.is_last_batch():
                self.producer_queue.send(batch.encode())
                return
            try:
                batch.change_header_name_value(self.column_id, self.column_name, self.join_dictionary)
            except (ValueError, KeyError) as e:
                logging.error(
                    f'action: join_batch_with_dicctionary | result: fail | error: {e}')
            self.producer_queue.send(batch.encode())

    def close(self):
        # try:
        #     if self.consumer_queue:
        #         self.consumer_queue.stop_consuming()
        #         self.consumer_queue.close()
        #     if self.producer_queue:
        #         self.producer_queue.close()
        #     if self.join_queue:
        #         self.join_queue.close()
        # except:
        #     pass
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
            except Exception:
                pass

            # Cerrar cache si corresponde
            if isinstance(self.join_dictionary, Cache):
                try:
                    self.join_dictionary.close()
                except Exception:
                    pass

