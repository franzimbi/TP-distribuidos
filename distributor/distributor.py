import os
import socket
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import send_batch
from common.batch import Batch

Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")

END_MARKER = b"&END&"

shutdown = threading.Event()


class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket

        self.producer_queues = {}
        self.consumer_queues = {}

        self.producer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_producer)
        self.consumer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_consumer)

    def add_client(self, client_id, client_socket):
        self.number_of_clients += 1
        self.clients[client_id] = client_socket

    def remove_client(self, client_id):
        sock = self.clients.pop(client_id, None)
        if sock:
            self.number_of_clients -= 1
        return sock

    def distribute_batch_to_workers(self, query_id, batch: Batch):
        if query_id != 1:
            # por ahora solo Q1 activa
            print(f"[DISTRIBUTOR] Query {query_id} no configurada.")
            return

        producer_queue = self.producer_queues[1]
        # if batch.is_last_batch():
        #     producer_queue.send(END_MARKER)
        #     print("[DISTRIBUTOR] Marcador END enviado a workers.")
        #     return

        producer_queue.send(batch.encode())

    def callback(self, ch, method, properties, body: bytes):
        client_id = 1  # por ahora fijo pq un solo cliente
        client_socket = self.clients.get(client_id)

        batch = Batch()
        batch.decode(body)
        try:
            send_batch(client_socket, batch)
        except Exception as e:
            print(f"[DISTRIBUTOR] error al querer enviar batch:{batch} al cliente:{client_id} | error: {e}")
        if batch.is_last_batch():
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
                self.remove_client(client_id)
            except Exception as e:
                print(f"[DISTRIBUTOR] Error cerrando socket del cliente{client_id} | error: {e}")
            return

    def start_consuming_from_workers(self, query_id):
        """
        Arranca el consumo desde la última cola de la query (q14 para Q1)
        """
        cq = self.consumer_queues.get(query_id)
        if cq is None:
            return
        try:
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
