import logging
import os
import signal
import sys
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import send_batch
from common.batch import Batch

COUNT_OF_PRINTS = 30000

Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")

Q3queue_consumer = os.getenv("CONSUME_QUEUE_Q3")
Q3queue_producer = os.getenv("PRODUCE_QUEUE_Q3")
Q31queue_joiner = os.getenv("JOIN_QUEUE_Q3.1")
Q32queue_joiner = os.getenv("JOIN_QUEUE_Q3.2")

Q4queue_consumer = os.getenv("CONSUME_QUEUE_Q4")
Q4queue_producer = os.getenv("PRODUCE_QUEUE_Q4")
Q4queue_joiner_users = os.getenv("JOIN_QUEUE_Q4_1")
Q4queue_joiner_stores = os.getenv("JOIN_QUEUE_Q4_2")
Q4queue_joiner_users2 = os.getenv("JOIN_QUEUE_Q4_1_2")
Q4queue_joiner_stores2 = os.getenv("JOIN_QUEUE_Q4_2_2")

shutdown = threading.Event()


class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket
        self.files_types_for_queries = {'t': [1,3,4], 's': [3,32,42,44],
                                        'u': [4,43]}  # key: type_file, value: list of query_ids

        self.producer_queues = {}  # key: query_id, value: MessageMiddlewareQueue
        self.consumer_queues = {}  # ""
        self.joiner_queues = {}  # ""

        signal.signal(signal.SIGTERM, self.graceful_quit)

        self.producer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_producer)
        self.consumer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_consumer)

        self.producer_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_producer)
        self.consumer_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_consumer)
        self.joiner_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q31queue_joiner)
        self.joiner_queues[32] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q32queue_joiner)

        self.producer_queues[4] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q4queue_producer)
        self.consumer_queues[4] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q4queue_consumer)
        self.joiner_queues[4] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q4queue_joiner_users)
        self.joiner_queues[42] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q4queue_joiner_stores)
        self.joiner_queues[43] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q4queue_joiner_users2)
        self.joiner_queues[44] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q4queue_joiner_stores2)

    def graceful_quit(self, signum, frame):
        logging.info(f'signum {signum} activado')
        shutdown.set()
        self.stop_consuming_from_all_workers()

        for qid, q in {**self.consumer_queues, **self.producer_queues, **self.joiner_queues}.items():
            if q is not None:
                try:
                    q.close()
                except Exception as e:
                    logging.error(f"[DISTRIBUTOR] Error cerrando conexión {qid}: {e}")

        for cid, sock in self.clients.items():
            try:
                sock.close()
            except Exception as e:
                logging.error(f"[DISTRIBUTOR] Error cerrando socket cliente {cid}: {e}")
        sys.exit(0)

    def add_client(self, client_id, client_socket):
        self.number_of_clients += 1
        self.clients[client_id] = client_socket

    def remove_client(self, client_id):
        sock = self.clients.pop(client_id, None)
        if sock:
            self.number_of_clients -= 1
        return sock

    def distribute_batch_to_workers(self, batch: Batch):
        queries = self.files_types_for_queries[batch.type()]
        if batch.id() % COUNT_OF_PRINTS == 0 or batch.id() == 0:
            logging.debug(
                f"[DISTRIBUTOR] Distribuyendo batch {batch.id()} de tipo {batch.type()} a las queries {queries}.")
        for query_id in queries:
            batch.set_query_id(query_id)
            if batch.type() == 't':  # la proxima veo d hacer algo mas objetoso para evitar estos ifs ~pedro
                q = self.producer_queues[query_id]
            if batch.type() == 's':
                q = self.joiner_queues[query_id]
            if batch.type() == 'u':
                q = self.joiner_queues[4]
            q.send(batch.encode())

    def callback(self, ch, method, properties, body: bytes):
        batch = Batch()
        batch.decode(body)
        client_id = 1  # por ahora fijo pq un solo cliente
        client_socket = self.clients.get(client_id)
        try:
            send_batch(client_socket, batch)
            # if batch.get_query_id() == 4:
            #     print(f"\n\n[DISTRIBUTOR] Enviado batch {batch.id()} de tipo {batch.type()} a cliente {client_id} (Q{batch.get_query_id()})\n\n")
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
            if not shutdown.is_set():
                logging.error(f"[DISTRIBUTOR] Error en consumo de workers: {e}")

    def stop_consuming_from_all_workers(self):
        for qid, cq in self.consumer_queues.items():
            if cq is not None:
                try:
                    cq.stop_consuming()
                except Exception as e:
                    logging.error(f"[DISTRIBUTOR] Error al detener consumo de la query {qid}: {e}")
