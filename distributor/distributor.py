import os
import socket
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import send_batch
from common.batch import Batch

Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")

Q3queue_consumer = os.getenv("CONSUME_QUEUE_Q3")
Q3queue_producer = os.getenv("PRODUCE_QUEUE_Q3")
Q3queue_joiner = os.getenv("JOIN_QUEUE_Q3")

shutdown = threading.Event()
class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket
        self.files_types_for_queries = {'t': [1,3], 's': [3]}  # key: type_file, value: list of query_ids

        self.producer_queues = {} # key: query_id, value: MessageMiddlewareQueue
        self.consumer_queues = {} # ""
        self.joiner_queues = {}   # ""

        self.producer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_producer)
        self.consumer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_consumer)
        
        self.producer_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_producer)        
        self.consumer_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_consumer)
        self.joiner_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_joiner)



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
        # print(f"[DISTRIBUTOR] Distribuyendo batch {batch.id()} de tipo {batch.type()} a las queries {queries}.")
        for query_id in queries:
            batch.set_query_id(query_id)
            if query_id == 3 and (batch.id() % 10000 == 0 or batch.id() == 0):
                print(f"\n[DISTRIBUTOR] Distribuyendo batch {batch.id()} de tipo {batch.type()} a la query {batch.get_query_id()}.")
            if(batch.type() == 't'):
                q = self.producer_queues[query_id]
            if(batch.type() == 's'):
                q = self.joiner_queues[query_id]
            q.send(batch.encode())

    def callback(self, ch, method, properties, body: bytes):
        client_id = 1  # por ahora fijo pq un solo cliente
        client_socket = self.clients.get(client_id)

        batch = Batch()
        batch.decode(body)
        try:
            send_batch(client_socket, batch)
            if batch.get_query_id() == 3:
                print(f"[DISTRIBUTOR] Enviado batch {batch.id()} de tipo {batch.type()} a cliente {client_id} (Q{batch.get_query_id()})")
        except Exception as e:
            print(f"[DISTRIBUTOR] error al querer enviar batch:{batch} al cliente:{client_id} | error: {e}")
        if batch.is_last_batch():
            print(f"\n\n\n[DISTRIBUTOR] Cliente {client_id} recibió todos los resultados de la query {batch.get_query_id()}.\n\n\n")
            # try:
                # client_socket.shutdown(socket.SHUT_RDWR)
                # client_socket.close()
                # self.remove_client(client_id)
            # except Exception as e:
            #     print(f"[DISTRIBUTOR] Error cerrando socket del cliente{client_id} | error: {e}")
            return

    def start_consuming_from_workers(self, query_id):
        cq = self.consumer_queues.get(query_id)
        if cq is None:
            return
        try:
            print(f"\n\n\n[DISTRIBUTOR] Iniciando consumo de la query {query_id}.\n\n\n")
            cq.start_consuming(self.callback)
        except Exception as e:
            if not shutdown.is_set():
                print(f"[DISTRIBUTOR] Error en consumo de workers: {e}")

    def stop_consuming_from_all_workers(self):
        for qid, cq in self.consumer_queues.items():
            if cq is not None:
                try:
                    cq.stop_consuming()
                    print(f"[DISTRIBUTOR] Detenido consumo de la query {qid}.")
                except Exception as e:
                    print(f"[DISTRIBUTOR] Error al detener consumo de la query {qid}: {e}")