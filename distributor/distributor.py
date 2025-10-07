import logging
import os
import signal
import sys
import threading
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from common.protocol import send_batch
from common.batch import Batch

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
            route_keys=[''], exchange_type='fanout', queue_name='products'
        )
        self.stores = MessageMiddlewareExchange(
            host='rabbitmq', exchange_name=storesExchange,
            route_keys=[''], exchange_type='fanout', queue_name='stores'
        )
        self.users = MessageMiddlewareExchange(
            host='rabbitmq', exchange_name=usersExchange,
            route_keys=[''], exchange_type='fanout', queue_name='users'
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
        self.threads_queries['q1'] = threading.Thread(target=self.q1_results.start_consuming(self.callback), args=(1,),
                                                      daemon=True)
        self.threads_queries['q21'] = threading.Thread(target=self.q21_results.start_consuming(self.callback),
                                                       args=(21,), daemon=True)
        self.threads_queries['q22'] = threading.Thread(target=self.q22_results.start_consuming(self.callback),
                                                       args=(22,), daemon=True)
        self.threads_queries['q3'] = threading.Thread(target=self.q3_results.start_consuming(self.callback), args=(3,),
                                                      daemon=True)
        self.threads_queries['q4'] = threading.Thread(target=self.q4_results.start_consuming(self.callback), args=(4,),
                                                      daemon=True)

        for _, t in self.threads_queries.items():
            t.start()

    def stop_consuming_from_all_workers(self):
        self.transactions.close()
        self.transaction_items.close()
        self.stores.close()
        self.products.close()
        self.users.close()

        self.q1_results.stop_consuming()
        self.q1_results.close()

        self.q21_results.stop_consuming()
        self.q21_results.close()

        self.q22_results.stop_consuming()
        self.q22_results.close()

        self.q3_results.stop_consuming()
        self.q3_results.close()

        self.q4_results.stop_consuming()
        self.q4_results.close()

        for _, t in self.threads_queries.items():
            t.join()


