import logging
import os
import signal
import sys
import threading
from time import time
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from common.protocol import send_batch
from common.batch import Batch
from functools import partial

COUNT_OF_PRINTS = 10000

transactionsQueue = os.getenv('transactionsQueue')
itemsQueue = os.getenv('itemsQueue')
productsExchange = os.getenv('productsExchange')
storesExchange = os.getenv('storesExchange')
usersExchange = os.getenv('usersExchange')

Q1_results = os.getenv('Q1result')
Q21_results = os.getenv('Q21result')
Q22_results = os.getenv('Q22result')
Q3_results = os.getenv('Q3result')
Q4_results = os.getenv('Q4result')

shutdown = threading.Event()


class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket

        self.transactions = MessageMiddlewareQueue(host='rabbitmq', queue_name=transactionsQueue)
        self.transaction_items = MessageMiddlewareQueue(host='rabbitmq', queue_name=itemsQueue)
        #
        self.products = MessageMiddlewareExchange(
            host='rabbitmq', exchange_name=productsExchange,
            route_keys=[''], exchange_type='fanout', queue_name=None
        )
        self.stores = MessageMiddlewareExchange(
            host='rabbitmq', exchange_name=storesExchange,
            route_keys=[''], exchange_type='fanout', queue_name=None
        )
        self.users = MessageMiddlewareExchange(
            host='rabbitmq', exchange_name=usersExchange,
            route_keys=[''], exchange_type='fanout', queue_name=None
        )
        self.route = {
            't': self.transactions,
            'i': self.transaction_items,
            'm': self.products,
            's': self.stores,
            'u': self.users,
        }

        self.q1_results = MessageMiddlewareQueue('rabbitmq', Q1_results)
        self.q21_results = MessageMiddlewareQueue('rabbitmq', Q21_results)
        self.q22_results = MessageMiddlewareQueue('rabbitmq', Q22_results)
        self.q3_results = MessageMiddlewareQueue('rabbitmq', Q3_results)
        self.q4_results = MessageMiddlewareQueue('rabbitmq', Q4_results)

        self.threads_queries = {}

        self.lock = threading.Lock()

    def add_client(self, client_socket):
        self.number_of_clients += 1
        self.clients[self.number_of_clients] = client_socket
        return self.number_of_clients

    def remove_client(self, client_id):
        sock = self.clients.pop(client_id, None)
        if sock:
            self.number_of_clients -= 1
        return sock

    def distribute_batch_to_workers(self, batch: Batch):
        if batch.is_last_batch():
            logging.debug(
                f"[DISTRIBUTOR] Distribuido batch final {batch.id()} de tipo {batch.type()} de client{batch.client_id()}.")

        destination = self.route.get(batch.type())
        if destination:
            with self.lock:
                destination.send(batch.encode())
        else:
            logging.error(f'[DISTRIBUTION] Unknown transaction type: {batch.type()}')

    def callback(self, ch, method, properties, body: bytes, query_id):
        batch = Batch(); batch.decode(body)
        batch.set_query_id(query_id)
        client_id = batch.client_id()
        client_socket = self.clients.get(client_id)
        if client_socket is None:
            logging.error(f"[DISTRIBUTOR] No existe el cliente {client_id} para enviarle los resultados.")
            return
        try:
            # logging.debug(f'[DISTRIBUTOR] enviando el batch de resultado {query_id} al client {client_id}, con id {batch.id()}')
            send_batch(client_socket, batch)
        except Exception as e:
            logging.error(f"[DISTRIBUTOR] error al querer enviar batch:{batch} al cliente:{client_id} | error: {e}")
        if batch.is_last_batch():
            logging.debug(
                f"\n[DISTRIBUTOR] Cliente {client_id} recibió todos los resultados de la query {query_id}.\n")
            return

    def start_consuming_from_workers(self):
        # TODO: lanza los hilos de start_consuming de las 4 queries y se las manda al client por id
        # for clave, valor in self.route.items():
        def helper_callback(query_id):
            return lambda ch, method, properties, body: self.callback(ch, method, properties, body, query_id)
        
        self.threads_queries['q1']  = threading.Thread(target=lambda: self.q1_results.start_consuming(helper_callback(1)),  daemon=True)
        # self.threads_queries['q21'] = threading.Thread(target=lambda: self.q21_results.start_consuming(helper_callback(21)), daemon=True)
        # self.threads_queries['q22'] = threading.Thread(target=lambda: self.q22_results.start_consuming(helper_callback(22)), daemon=True)
        self.threads_queries['q3']  = threading.Thread(target=lambda: self.q3_results.start_consuming(helper_callback(3)),  daemon=True)
        self.threads_queries['q4']  = threading.Thread(target=lambda: self.q4_results.start_consuming(helper_callback(4)),  daemon=True)

        for _, t in self.threads_queries.items():
            t.start()

    def stop_consuming_from_all_workers(self):
        middlewares = [
            self.transactions,
            self.transaction_items,
            self.stores,
            self.products,
            self.users,
            self.q1_results,
            self.q21_results,
            self.q22_results,
            self.q3_results,
            self.q4_results
        ]
        for mw in middlewares:
            try:
                if hasattr(mw, "stop_consuming"):
                    try:
                        mw.stop_consuming()
                    except Exception as e:
                        logging.debug(f"[Distributor] Error deteniendo consumo de {mw}: {e}")
                try:
                    mw.close()
                except Exception as e:
                    logging.debug(f"[Distributor] Error cerrando middleware {mw}: {e}")
            except Exception as e:
                logging.debug(f"[Distributor] Error general cerrando middleware {mw}: {e}")

        for _, t in self.threads_queries.items():
            try:
                t.join()
            except Exception as e:
                logging.debug(f"[Distributor] Error al join thread {t}: {e}")

