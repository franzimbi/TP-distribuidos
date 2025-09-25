import os
import socket
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import decode_batch
from common.batch import Batch

Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")

END_MARKER = b"&END&"

shutdown = threading.Event()

class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}  # key: client_id, value: socket

        # map de producers/consumers por query
        self.producer_queues = {}
        self.consumer_queues = {}

        # Inicializar colas de MQ
        self.producer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_producer)
        self.consumer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_consumer)

    def add_client(self, client_id, client_socket):
        self.number_of_clients += 1
        self.clients[client_id] = client_socket
        print(f"[DISTRIBUTOR] Cliente {client_id} conectado. Total clientes: {self.number_of_clients}")


    def distribute_batch_to_workers(self, query_id, batch: Batch):
        """
        Convierte el Batch (nuestro framing) al formato viejo (string con líneas CSV unidas por '|')
        y lo publica en la primera cola del pipeline (q11).
        Si es el último, envía '&END&'.
        """
        if query_id != 1:
            # por ahora solo Q1 activa
            print(f"[DISTRIBUTOR] Query {query_id} no configurada.")
            return

        producer_queue = self.producer_queues[1]
        if batch.is_last_batch():
            producer_queue.send(END_MARKER)
            print("[DISTRIBUTOR] Marcador END enviado a workers.")
            return

        # body: lista de filas (listas/strings) -> a CSV por fila -> unir con '|'
        lines = []
        for row in batch._body:
            # row puede venir como lista de strings; aseguramos csv
            if isinstance(row, (list, tuple)):
                lines.append(",".join(row))
            else:
                # fallback si ya era string CSV
                lines.append(str(row).strip())

        payload = "|".join(lines).encode("utf-8")
        producer_queue.send(payload)
        if batch.id() % 300 == 0:
            print(f"[DISTRIBUTOR] Batch {batch.id()} distribuido a la cola de la query {query_id}.")

    def _on_worker_msg_q1(self, ch, method, properties, body: bytes):
        client_id = 1  # por ahora 1 cliente
        client_socket = self.clients.get(client_id)

        if client_socket is None:
            print("[DISTRIBUTOR] No hay socket de cliente disponible para enviar resultados.")
            return

        # Si el cliente ya cerró de su lado o el socket está inválido, evitamos romper
        try:
            _ = client_socket.fileno()
        except Exception:
            print("[DISTRIBUTOR] Socket de cliente inválido.")
            return

        if body == END_MARKER:
            end_batch = Batch(id=0, last=True, type_file='t', header=[], rows=[])
            try:
                client_socket.sendall(end_batch.encode())
                print("[DISTRIBUTOR] END reenviado al cliente. Cerrando socket del cliente.")
            except Exception as e:
                print(f"[DISTRIBUTOR] Error enviando END al cliente: {e}")
            # AHORA sí cerramos y removemos
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
                if self.clients.get(client_id) is client_socket:
                    del self.clients[client_id]
            except Exception:
                print("[DISTRIBUTOR] Error cerrando socket del cliente.")
            return

        rows = decode_batch(body)  # [['c1','c2',...], ...]
        out_batch = Batch(id=0, last=False, type_file='t', header=[], rows=rows)

        try:
            client_socket.sendall(out_batch.encode())
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
            cq.start_consuming(self._on_worker_msg_q1)
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