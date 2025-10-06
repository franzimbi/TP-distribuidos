import logging
import os
import signal
import sys
import threading
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from common.protocol import send_batch
from common.batch import Batch

COUNT_OF_PRINTS = 10000

transactionsExchange = os.getenv('transactionsExchange')
itemsExchange = os.getenv('itemsExchange')
productsExchange = os.getenv('productsExchange')
storesExchange = os.getenv('storesExchange')
usersExchange = os.getenv('usersExchange')

Q1_results = os.getenv('Queue_final_Q1')
Q21_results = os.getenv('Queue_final_Q21')
Q22_results = os.getenv('Queue_final_Q22')
Q3_results = os.getenv('Queue_final_Q3')
Q4_results = os.getenv('Queue_final_Q4')

shutdown = threading.Event()


class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket
        signal.signal(signal.SIGTERM, self.graceful_quit)

        self.transactions = MessageMiddlewareExchange(host='rabbitmq', exchange_name=transactionsExchange,
                                                      route_keys='', exchange_type='fanout', queue_name='transactions')
        self.transaction_items = MessageMiddlewareExchange(host='rabbitmq', exchange_name=itemsExchange, route_keys='',
                                                           exchange_type='fanout', queue_name='items')
        self.products = MessageMiddlewareExchange(host='rabbitmq', exchange_name=productsExchange, route_keys='',
                                                  exchange_type='fanout', queue_name='products')
        self.stores = MessageMiddlewareExchange(host='rabbitmq', exchange_name=storesExchange, route_keys='',
                                                exchange_type='fanout', queue_name='stores')
        self.users = MessageMiddlewareExchange(host='rabbitmq', exchange_name=usersExchange, route_keys='',
                                               exchange_type='fanout', queue_name='users')

        self.route = {
            't': self.transactions,
            'r': self.transaction_items,
            's': self.stores,
            'u': self.users,
            'i': self.products,
        }

        self.q1_results = MessageMiddlewareQueue('rabbitmq', Q1_results)
        self.q21_results = MessageMiddlewareQueue('rabbitmq', Q21_results)
        self.q22_results = MessageMiddlewareQueue('rabbitmq', Q22_results)
        self.q3_results = MessageMiddlewareQueue('rabbitmq', Q3_results)
        self.q4_results = MessageMiddlewareQueue('rabbitmq', Q4_results)

    def graceful_quit(self, signum, frame):
        logging.info(f'signum {signum} activado')
        # shutdown.set()
        # self.stop_consuming_from_all_workers()
        #
        # for qid, q in {**self.consumer_queues, **self.producer_queues, **self.joiner_queues}.items():
        #     if q is not None:
        #         try:
        #             q.close()
        #         except Exception as e:
        #             logging.error(f"[DISTRIBUTOR] Error cerrando conexión {qid}: {e}")
        #
        # for cid, sock in self.clients.items():
        #     try:
        #         sock.close()
        #     except Exception as e:
        #         logging.error(f"[DISTRIBUTOR] Error cerrando socket cliente {cid}: {e}")
        # sys.exit(0)

    def add_client(self, client_id, client_socket):
        self.number_of_clients += 1
        self.clients[client_id] = client_socket

    def remove_client(self, client_id):
        sock = self.clients.pop(client_id, None)
        if sock:
            self.number_of_clients -= 1
        return sock

    def distribute_batch_to_workers(self, batch: Batch):
        if batch.is_last_batch():
            logging.debug(f"[DISTRIBUTOR] Distribuido batch final {batch.id()} de tipo {batch.type()} de client{batch.client_id()}.")

        destination = self.route.get(batch.type())
        if destination:
            destination.send(batch.encode())
        else:
            logging.error(f'[DISTRIBUTION] Unknown transaction type: {type}')

    def callback(self, ch, method, properties, body: bytes):
        batch = Batch()
        batch.decode(body)
        # if batch.id() % COUNT_OF_PRINTS == 0 or batch.id() == 0:
        #     logging.debug(f"[DISTRIBUTOR] ENVIANDO batch {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()} AL CLIENTE.")
        client_id = 1  # por ahora fijo pq un solo cliente
        client_socket = self.clients.get(client_id)
        try:
            send_batch(client_socket, batch)
        except Exception as e:
            logging.error(f"[DISTRIBUTOR] error al querer enviar batch:{batch} al cliente:{client_id} | error: {e}")
        if batch.is_last_batch():
            logging.debug(
                f"\n[DISTRIBUTOR] Cliente {client_id} recibió todos los resultados de la query {batch.get_query_id()}.\n")
            return

    def start_consuming_from_workers(self, query_id):
        cq = self.consumer_queues.get(query_id)
        if cq is None:
            return
        try:
            cq.start_consuming(self.callback)
        except Exception as e:
            pass
            # if not shutdown.is_set():
            #     logging.error(f"[DISTRIBUTOR] Error en consumo de workers: {e}")

    def stop_consuming_from_all_workers(self):
        for qid, cq in self.consumer_queues.items():
            if cq is not None:
                try:
                    cq.stop_consuming()
                except Exception as e:
                    pass
                    # logging.error(f"[DISTRIBUTOR] Error al detener consumo de la query {qid}: {e}")
