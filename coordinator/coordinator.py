import os
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch

class Coordinator:
    def __init__(self):
        self.num_nodes = int(os.getenv("NUM_NODES", "1"))
        host = os.getenv("RABBITMQ_HOST", "rabbitmq")

        self.consumer_queue = None
        self.producer_queues = {}
        self.threads = []

        cq = os.getenv(f"CONSUME_QUEUE")
        self.consumer_queue = MessageMiddlewareQueue(host=host, queue_name=cq)

        for i in range(1, self.num_nodes + 1):
            pq = os.getenv(f"PRODUCE_QUEUE_{i}")
            if not cq or not pq:
                raise ValueError(f"Missing CONSUME_QUEUE_{i} or PRODUCE_QUEUE_{i}")
            self.producer_queues[i] = MessageMiddlewareQueue(host=host, queue_name=pq)

        downstream_q = os.getenv("DOWNSTREAM_QUEUE", "downstream")
        self.downstream_queue = MessageMiddlewareQueue(host=host, queue_name=downstream_q)

        self.end_batch = None
        self.finished_nodes = 0

    def start(self):
        print(f"\n\n\n\n\n\n[Coordinator] Listening queue...\n\n\n\n\n")
        self.consumer_queue.start_consuming(self.callback)


    def callback(self, ch, method, properties, body):
        print(f"\n\n[Coordinator] Message received from node\n\n")
        try:
            #intento decodificar como texto
            msg = body.decode()
            if msg.startswith("NODE_FINISHED"):
                self.finished_nodes += 1
                print(f"[Coordinator] Node {self.finished_nodes} finished ({self.finished_nodes}/{self.num_nodes})")

                if self.finished_nodes == self.num_nodes:
                    self._send_downstream_end()
            else:
                print(f"[Coordinator] Unknown text message: {msg}")

        except UnicodeDecodeError:
            #si no es texto, lo tomo como Batch
            batch = Batch()
            batch.decode(body)

            if batch.is_last_batch():
                print(f"[Coordinator] Received END batch from node")
                self.end_batch = batch
                self._broadcast_flush()

    def _broadcast_flush(self):
        for node_id, producer in self.producer_queues.items():
            producer.send("FLUSH")
        print("[Coordinator] Sent FLUSH to all nodes")

    def _send_downstream_end(self):
        self.downstream_queue.send(self.end_batch.encode())
        print("[Coordinator] Sent END_BATCH downstream")
        print(f"\n el batch es:\n {self.end_batch} \n\n")

    def close(self):
        self.consumer_queue.stop_consuming()
        self.consumer_queue.close()
        print("[Coordinator] consumer queue closed.")
        for q in list(self.producer_queues.values()):
            q.close()
