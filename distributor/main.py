#!/usr/bin/env python3
import os
import socket
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import Batch, recv_batches_from_socket, decode_batch

# Colas (Q1 pipeline)
Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")   # p.ej. q14 (salida última etapa)
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")   # p.ej. q11 (entrada primera etapa)

shutdown = threading.Event()

class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}

        self.producer_queues = {}
        self.consumer_queues = {}

        # Query 1 activa
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
            producer_queue.send(b"&END&")
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

        if body == b"&END&":
            end_batch = Batch(id=0, last=True, type_file='t', header=[], rows=[])
            try:
                client_socket.sendall(end_batch.encode())
                print("[DISTRIBUTOR] END reenviado al cliente. Cerrando socket del cliente.")
            except Exception as e:
                print(f"[DISTRIBUTOR] Error enviando END al cliente: {e}")
            # AHORA sí cerramos y removemos
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                client_socket.close()
            except Exception:
                pass
            # quitar del mapa
            if self.clients.get(client_id) is client_socket:
                del self.clients[client_id]
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

distributor = Distributor()

TCP_PORT = int(os.getenv("TCP_PORT", 5000))
HOST = "0.0.0.0"
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, TCP_PORT))
server_socket.listen()
server_socket.settimeout(1.0)  # para poder salir con shutdown

print(f"Distributor escuchando en {HOST}:{TCP_PORT}")

def handle_client(sock, addr):
    print(f"Cliente conectado: {addr}")
    # por ahora usamos client_id = 1 fijo
    client_id = 1
    distributor.add_client(client_id, sock)

    try:
        query_id = 1
        for batch in recv_batches_from_socket(sock):
            distributor.distribute_batch_to_workers(query_id, batch)
            if batch.is_last_batch():
                print(f"[DISTRIBUTOR] Cliente {addr} terminó de ENVIAR (esperando resultados de workers).")
                break
    except Exception as e:
        print(f"Error con cliente {addr}: {e}")
        # si hubo error, cerremos y removamos
        try:
            sock.close()
        except Exception:
            pass
        # remover del map si estaba
        if distributor.clients.get(client_id) is sock:
            del distributor.clients[client_id]
        print(f"Cliente desconectado (por error): {addr}")
    # NO cerrar aquí en el caso normal: se cierra cuando llega END desde workers


def accept_clients():
    threads = []
    while not shutdown.is_set():
        try:
            sock, addr = server_socket.accept()
        except socket.timeout:
            continue
        t = threading.Thread(target=handle_client, args=(sock, addr), daemon=True)
        t.start()
        threads.append(t)
    # Esperar que terminen si estamos apagando
    for t in threads:
        t.join(timeout=1.0)

# Hilos: consumidor de workers y acceptor
q1_consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(1,), daemon=True)
q1_consumer_thread.start()

accept_thread = threading.Thread(target=accept_clients, daemon=True)
accept_thread.start()

try:
    # Quedarse en el main hasta Ctrl+C (o que el contenedor muera)
    while True:
        accept_thread.join(timeout=1.0)
        q1_consumer_thread.join(timeout=1.0)
except KeyboardInterrupt:
    pass
finally:
    shutdown.set()
    distributor.stop_consuming_from_all_workers()
    try:
        server_socket.close()
    except Exception:
        pass
    accept_thread.join(timeout=2.0)
    q1_consumer_thread.join(timeout=2.0)
    print("[DISTRIBUTOR] Apagado limpio.")
