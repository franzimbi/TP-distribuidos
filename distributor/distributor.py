import os
import socket
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import decode_batch
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
        print(f"[DISTRIBUTOR] Cliente {client_id} conectado. Total clientes: {self.number_of_clients}")


    def distribute_batch_to_workers(self, query_id, batch: Batch):
        if query_id != 1:
            # por ahora solo Q1 activa
            print(f"[DISTRIBUTOR] Query {query_id} no configurada.")
            return

        producer_queue = self.producer_queues[1]
        if batch.is_last_batch():
            producer_queue.send(END_MARKER)
            print("[DISTRIBUTOR] Marcador END enviado a workers.")
            return
        
        producer_queue.send(batch.encode())
        if batch.id() % 300 == 0:
            print(f"[DISTRIBUTOR] Batch {batch.id()} distribuido a la cola de la query {query_id}.")

    def callback(self, ch, method, properties, body: bytes):
        client_id = 1 #por ahora fijo pq un solo cliente
        client_socket = self.clients.get(client_id)
        if body == END_MARKER:
            end_batch = Batch(id=0, last=True, type_file='t', header=[], rows=[])
            try:
                send_batch(client_socket, end_batch)
                print("[DISTRIBUTOR] END reenviado al cliente. Cerrando socket del cliente.")
            except Exception as e:
                print(f"[DISTRIBUTOR] Error enviando END al cliente: {e}")
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
                if self.clients.get(client_id) is client_socket:
                    del self.clients[client_id]
            except Exception:
                print("[DISTRIBUTOR] Error cerrando socket del cliente.")
            return

        batch = Batch(type_file='t')
        batch.decode(body)
        #aca en teoria me fijo el id del client, el d d la query, lo q sea q necesite 
        print(f"[DISTRIBUTOR] Recibido batch de los workers.")

        try:
            send_batch(client_socket, batch)
            print("[DISTRIBUTOR] Lote procesado reenviado al cliente.")
        except Exception as e:
            print(f"[DISTRIBUTOR] Error enviando batch al cliente: {e}")

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