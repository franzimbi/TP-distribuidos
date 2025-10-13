import logging
import os
import signal
import sys
import threading
from time import time
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from common.protocol import send_batch, send_joins_confirmation_to_client
from common.batch import Batch
from functools import partial

COUNT_OF_PRINTS = 10000

transactionsQueue = os.getenv('transactionsQueue')
itemsQueue = os.getenv('itemsQueue')
# productsExchange = os.getenv('productsExchange')
# storesExchange = os.getenv('storesExchange')
# usersExchange = os.getenv('usersExchange')

Q1_results = os.getenv('Q1result')
Q21_results = os.getenv('Q21result')
Q22_results = os.getenv('Q22result')
Q3_results = os.getenv('Q3result')
Q4_results = os.getenv('Q4result')

# q3_Stores1 = os.getenv('q3Stores1')
# q4_Stores1 = os.getenv('q4Stores1')
# q4_Users1 = os.getenv('q4Users1')
# q4_Users2 = os.getenv('q4Users2')
# q4_Users3 = os.getenv('q4Users3')
# q21_products1 = os.getenv('q21Products1')
# q22_products1 = os.getenv('q22Products1')

q_stores = os.getenv('storesQueues')
q_products = os.getenv('productsQueues')
q_users = os.getenv('usersQueues')

queues_stores_list = []
for q in q_stores.split(','):
    queues_stores_list.append(q.strip())

queue_products_list = []
for q in q_products.split(','):
    queue_products_list.append(q.strip())

queue_users_list = []
for q in q_users.split(','):
    queue_users_list.append(q.strip())


q_joins_confirmation = os.getenv('CONFIRMATION_QUEUE')
number_of_joins = int(os.getenv('numberOfJoins'))


shutdown = threading.Event()


class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket

        self.transactions = MessageMiddlewareQueue(host='rabbitmq', queue_name=transactionsQueue)
        self.transaction_items = MessageMiddlewareQueue(host='rabbitmq', queue_name=itemsQueue)
        #
        # self.products = MessageMiddlewareExchange(
        #     host='rabbitmq', exchange_name=productsExchange,
        #     route_keys=[''], exchange_type='fanout', queue_name=None
        # )
        # self.stores = MessageMiddlewareExchange(
        #     host='rabbitmq', exchange_name=storesExchange,
        #     route_keys=[''], exchange_type='fanout', queue_name=None
        # )
        # self.users = MessageMiddlewareExchange(
        #     host='rabbitmq', exchange_name=usersExchange,
        #     route_keys=[''], exchange_type='fanout', queue_name=None
        # )

        self.stores_queues = []
        for q in queues_stores_list:
            self.stores_queues.append(MessageMiddlewareQueue(host='rabbitmq', queue_name=q))

        self.users_queues = []
        for q in queue_users_list:
            self.users_queues.append(MessageMiddlewareQueue(host='rabbitmq', queue_name=q))
        self.users_index = 0

        self.products_queues = []
        for q in queue_products_list:
            self.products_queues.append(MessageMiddlewareQueue(host='rabbitmq', queue_name=q))

        self.route = {
            't': self.transactions,
            'i': self.transaction_items,
        }

        self.q1_results = MessageMiddlewareQueue('rabbitmq', Q1_results)
        self.q21_results = MessageMiddlewareQueue('rabbitmq', Q21_results)
        self.q22_results = MessageMiddlewareQueue('rabbitmq', Q22_results)
        self.q3_results = MessageMiddlewareQueue('rabbitmq', Q3_results)
        self.q4_results = MessageMiddlewareQueue('rabbitmq', Q4_results)
        self.confirmation_queue = MessageMiddlewareQueue('rabbitmq', q_joins_confirmation)

        self.threads_queries = {}

        self.lock = threading.Lock()
        self.socket_lock = threading.Lock()
        
        self.client_counter = {}

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

        if batch.type() == 's':
            if batch.id() == 0 or batch.id() % COUNT_OF_PRINTS == 0:
                print(f"[DISTRIBUTION] Distribuyendo batch.id={batch.id()} de tipo 's' a {len(self.stores_queues)} colas de stores")
            for s in self.stores_queues:
                with self.lock:
                    s.send(batch.encode())
            return
        if batch.type() == 'u':
            if batch.id() == 0 or batch.id() % COUNT_OF_PRINTS == 0:
                print(f"[DISTRIBUTION] Distribuyendo batch.id={batch.id()} de tipo 'u' a {len(self.users_queues)} colas de users")
            if batch.is_last_batch():
                print(f"[DISTRIBUTION] Distribuyendo LASTbatch.id={batch.id()} de tipo 'u' a {len(self.users_queues)} colas de users")
                for u in self.users_queues:
                    with self.lock:
                        u.send(batch.encode())
                return
            with self.lock:
                self.users_queues[self.users_index].send(batch.encode())
                self.users_index = (self.users_index + 1) % len(self.users_queues)
            return
        if batch.type() == 'm':
            if batch.id() == 0 or batch.id() % COUNT_OF_PRINTS == 0:
                print(f"[DISTRIBUTION] Distribuyendo batch.id={batch.id()} de tipo 'm' a {len(self.products_queues)} colas de products")
            for p in self.products_queues:
                with self.lock:
                    p.send(batch.encode())
            return

        destination = self.route.get(batch.type())
        if destination:
            with self.lock:
                destination.send(batch.encode())
        else:
            logging.error(f'[DISTRIBUTION] Unknown transaction type: {batch.type()}')

    def callback(self, ch, method, properties, body: bytes, query_id):
        batch = Batch(); batch.decode(body)
        batch.set_query_id(query_id)
        print(f"[DISTRIBUTOR] Recibido batch de resultados {query_id} del worker, para el client {batch.client_id()}, con id {batch.id()}")
        client_id = batch.client_id()
        client_socket = self.clients.get(client_id)
        if client_socket is None:
            logging.error(f"[DISTRIBUTOR] No existe el cliente {client_id} para enviarle los resultados.")
            return
        try:
            # logging.debug(f'[DISTRIBUTOR] enviando el batch de resultado {query_id} al client {client_id}, con id {batch.id()}')
            
            with self.socket_lock: #TODO: cambiar este lock a un lock por cliente
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
        self.threads_queries['q21'] = threading.Thread(target=lambda: self.q21_results.start_consuming(helper_callback(21)), daemon=True)
        self.threads_queries['q22'] = threading.Thread(target=lambda: self.q22_results.start_consuming(helper_callback(22)), daemon=True)
        self.threads_queries['q3']  = threading.Thread(target=lambda: self.q3_results.start_consuming(helper_callback(3)),  daemon=True)
        self.threads_queries['q4']  = threading.Thread(target=lambda: self.q4_results.start_consuming(helper_callback(4)),  daemon=True)
        self.threads_queries['confirmations'] = threading.Thread(target=lambda: self.confirmation_queue.start_consuming(self.confirmation_callback), daemon=True)

        for _, t in self.threads_queries.items():
            t.start()

    def stop_consuming_from_all_workers(self):
        middlewares = [
            self.transactions,
            self.transaction_items,
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


    def confirmation_callback(self, ch, method, properties, body: bytes):
        print("hola, soy la callback confirmation")
        batch = Batch()
        batch.decode(body)
        print(f"[DISTRIBUTOR] Recibida confirmacion de joins del client_{batch.client_id()}")
        if batch.is_last_batch():
            print("es last batch")
            cid = batch.client_id()

            if cid not in self.client_counter:
                self.client_counter[cid] = 0

            self.client_counter[cid] += 1
            print(f"[DISTRIBUTOR] El client_{cid} tiene {self.client_counter[cid]} confirmaciones de joins.")
            if self.client_counter[cid] == number_of_joins:
                print(f"[DISTRIBUTOR] Enviando confirmacion de joins al client_{cid}")
                client_socket = self.clients.get(cid)
                with self.socket_lock:
                    send_joins_confirmation_to_client(client_socket)