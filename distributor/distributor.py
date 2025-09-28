import os
import socket
import threading
import logging
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import send_batch
from common.batch import Batch

Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")
Q3queue_consumer = os.getenv("CONSUME_QUEUE_Q3")
Q3queue_producer = os.getenv("PRODUCE_QUEUE_Q3")
Q3queue_joiner = os.getenv("JOIN_QUEUE_Q3")

shutdown = threading.Event()

ROUTING = {'t': [1, 3], 's': [3]}  

def make_batch_for_query(src: Batch, query_id: int) -> Batch:
    return Batch(
        id=src.id(),
        query_id=query_id,
        last=src.is_last_batch(),
        type_file=src.type(),
        header=list(src.get_header()),
        rows=[list(r) for r in src]
    )

class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {}
        self.client_queries = {}
        self._workers_started = False
        self._worker_threads = []

        self.producer_queues = {
            1: MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_producer),
            3: MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_producer),
        }
        self.joiner_queues = {
            3: MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_joiner),
        }
        self.consumer_queues = {
            1: MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_consumer),
            3: MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_consumer),
        }


    def add_client(self, client_id, client_socket):
        self.number_of_clients += 1
        self.clients[client_id] = client_socket
        logging.debug(f"[DISTRIBUTOR] Cliente agregado id={client_id}. Total={self.number_of_clients}")
        self._start_workers_once()

    def set_client_queries_for_type(self, client_id: int, type_file: str):
        """Inicializa las queries esperadas (los END que deben llegar) para este cliente."""
        qids = set(ROUTING.get(type_file, []))
        self.client_queries[client_id] = qids
        logging.debug(f"[DISTRIBUTOR] Cliente {client_id} espera END de queries={qids} para type={type_file}")

    def mark_query_end(self, client_id: int, query_id: int):
        """Marca que llegó el END de una query para este cliente."""
        pending = self.client_queries.get(client_id)
        if pending is None:
            logging.warning(f"[DISTRIBUTOR] mark_query_end sin 'pending' para cliente {client_id}")
            return False
        if query_id in pending:
            pending.discard(query_id)
            logging.debug(f"[DISTRIBUTOR] Cliente {client_id} END Q{query_id}. Pendientes={pending}")
        return len(pending) == 0  # True si ya no quedan pendientes

    def remove_client(self, client_id):
        sock = self.clients.pop(client_id, None)
        self.client_queries.pop(client_id, None)
        if sock:
            self.number_of_clients -= 1
        logging.debug(f"[DISTRIBUTOR] Cliente removido id={client_id}. Total={self.number_of_clients}")
        return sock

    def _start_workers_once(self):
        if self._workers_started:
            return
        self._workers_started = True
        for qid in (1, 3):
            cq = self.consumer_queues.get(qid)
            if not cq:
                logging.warning(f"[DISTRIBUTOR] No hay consumer queue para Q{qid}")
                continue
            t = threading.Thread(target=self.start_consuming_from_workers, args=(qid,), daemon=True)
            t.start()
            self._worker_threads.append(t)
        logging.debug("[DISTRIBUTOR] Consumers de workers iniciados.")


    def distribute_batch_to_workers(self, batch: Batch):
        query_ids = ROUTING.get(batch.type(), [])
        if not query_ids:
            logging.warning(f"[DISTRIBUTOR] Sin routing para type={batch.type()}")
            return
        for query_id in query_ids:
            if batch.type() == 's':
                logging.debug(f"\n\n\n[DISTRIBUTOR] Distribuyendo batch id={batch.id()} qid={query_id} a workers de Q{query_id}")
                q = self.joiner_queues.get(query_id)
                if not q:
                    logging.error(f"[DISTRIBUTOR] Joiner queue no configurada para Q{query_id}")
                    continue
                out = make_batch_for_query(batch, query_id)
                q.send(out.encode())
                logging.debug(f"[DISTRIBUTOR] Batch id={batch.id()} qid={query_id} enviado a joiner")
                continue
            q = self.producer_queues.get(query_id)
            if not q:
                logging.error(f"[DISTRIBUTOR] Producer queue no configurada para Q{query_id}")
                continue
            out = make_batch_for_query(batch, query_id)
            q.send(out.encode())

    def callback(self, ch, method, properties, body: bytes):
        client_id = 1  # único cliente por ahora
        client_socket = self.clients.get(client_id)

        batch = Batch()
        batch.decode(body)

        try:
            send_batch(client_socket, batch)
        except Exception as e:
            logging.error(f"[DISTRIBUTOR] Error al enviar batch id={batch.id()} qid={batch.get_query_id()} al cliente {client_id} | {e}")

        if batch.is_last_batch():
            query_id = batch.get_query_id()
            all_done = self.mark_query_end(client_id, query_id)
            if all_done:
                logging.debug(f"[DISTRIBUTOR] Todos los END recibidos para cliente {client_id}. Cerrando socket.")
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                    client_socket.close()
                except Exception:
                    pass
                self.remove_client(client_id)


    def start_consuming_from_workers(self, query_id):
        cq = self.consumer_queues.get(query_id)
        if cq is None:
            logging.error(f"[DISTRIBUTOR] start_consuming_from_workers sin cola para Q{query_id}")
            return
        try:
            cq.start_consuming(self.callback)
        except Exception as e:
            if not shutdown.is_set():
                logging.error(f"[DISTRIBUTOR] Error en consumo de workers (Q{query_id}): {e}")

    def stop_consuming_from_all_workers(self):
        for qid, cq in self.consumer_queues.items():
            if cq is not None:
                try:
                    cq.stop_consuming()
                    logging.debug(f"[DISTRIBUTOR] Detenido consumo de la query {qid}.")
                except Exception as e:
                    logging.error(f"[DISTRIBUTOR] Error al detener consumo de la query {qid}: {e}")
