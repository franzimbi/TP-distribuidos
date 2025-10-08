import os
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import logging
import sys
import signal

FLUSH_MESSAGE = 'FLUSH'
END_MESSAGE = 'END'


class Coordinator:
    def __init__(self, num_nodes, consumer, producers, downstreams):
        self.num_nodes = num_nodes
        self.consumer_queue = MessageMiddlewareQueue(host='rabbitmq', queue_name=consumer)
        self.downstream_queues = []
        for q in downstreams:
            self.downstream_queues.append(MessageMiddlewareQueue(host='rabbitmq', queue_name=q))
        # self.downstream_queue =
        self.producer_queues = {}

        for i in range(len(producers)):
            self.producer_queues[i] = MessageMiddlewareQueue(host='rabbitmq', queue_name=producers[i])

        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        self.end_batch = None
        self.finished_nodes = 0

    def graceful_shutdown(self, signum, frame):
        try:
            self.close()
            logging.info("[Coordinator] Apagado limpio.")
        except Exception as e:
            logging.error(f"[Coordinator] Error during shutdown: {e}")
        sys.exit(0)

    def start(self):
        logging.info("[Coordinator] Iniciando consumo...")
        self.consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        logging.info(f"[Coordinator] consumi algo !!")
        try:
            msg = body.decode('utf-8')
            logging.info(f"[Coordinator] Recibido mensaje de texto: {msg}")
            if len(body) <= 4:
                if msg == END_MESSAGE:
                    self.finished_nodes += 1
                    logging.info(f"[Coordinator] Nodo finalizó. Total nodos finalizados: {self.finished_nodes}/{self.num_nodes}")
                    if self.finished_nodes == self.num_nodes:
                        self._send_downstream_end()
                        self.end_batch = None
                        self.finished_nodes = 0
                        return
            else:
                raise UnicodeDecodeError("utf-8", body, 0, len(body), "No es END_MESSAGE")
            return
        except UnicodeDecodeError as e:
            logging.info(f"[Coordinator] No era un mensaje de texto, asumo que es un batch. Error: {e}")
            
            batch = Batch()
            batch.decode(body)
            logging.info(f"[Coordinator] Recibido batch {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()} y last_batch={batch.is_last_batch()}.")
            if batch.is_last_batch():
                self.end_batch = batch
                logging.info(f"[Coordinator] Recibido batch final {batch.id()} de tipo {batch.type()} de la query {batch.get_query_id()}.")
                self._broadcast_flush()
            else:
                logging.error(f"[COORDINATOR] Received batch {batch.id()} without last_batch flag. Ignoring.")

    def _broadcast_flush(self):
        for _, producer in self.producer_queues.items():
            producer.send(FLUSH_MESSAGE)
            logging.info("FLUSH enviado a un worker")

    def _send_downstream_end(self):
        logging.info(f"[Coordinator] Enviando batch final downstream {self.end_batch}")
        for i in self.downstream_queues:
            i.send(self.end_batch.encode())

    def close(self):
        self.consumer_queue.stop_consuming()
        self.consumer_queue.close()
        logging.info("[Coordinator] Consume queues cerradas")
        for q in list(self.producer_queues.values()):
            q.close()
        logging.info("[Coordinator] Producer queues cerradas")
        for i in self.downstream_queues:
           i.close()
        logging.info("[Coordinator] Downstream queues cerrada")
