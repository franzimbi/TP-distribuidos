import os
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch

FLUSH_MESSAGE = 'FLUSH'
END_MESSAGE = 'END'

class Coordinator:
    def __init__(self, num_nodes, consumer, producers, downstream):
        self.num_nodes = num_nodes
        self.consumer_queue = MessageMiddlewareQueue(host='rabbitmq', queue_name=consumer)
        self.downstream_queue = MessageMiddlewareQueue(host='rabbitmq', queue_name=downstream)
        self.producer_queues = {}
        
        for i in range(len(producers)):
            self.producer_queues[i+1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=producers[i])
        
        self.end_batch = None
        self.finished_nodes = 0


    def start(self):
        self.consumer_queue.start_consuming(self.callback) 

    def callback(self, ch, method, properties, body):
            try:
                msg = body.decode('utf-8')
                print(f"[Coordinator] Received message from node: |{msg}| and wait |{END_MESSAGE}|")
                if len(body) <= 4:
                    if msg == END_MESSAGE:
                        self.finished_nodes += 1
                        print(f"[Coordinator] Node {self.finished_nodes} finished ({self.finished_nodes}/{self.num_nodes})")
                        if self.finished_nodes == self.num_nodes:
                            self._send_downstream_end()
                            self.end_batch = None
                            self.finished_nodes = 0
                            return
                    else:
                        print(f"[Coordinator] Unknown text message: {msg}")
                    return
            except UnicodeDecodeError as e:
                batch = Batch()
                batch.decode(body)
                if batch.is_last_batch():
                    print(f"[Coordinator] Received END batch from node")
                    self.end_batch = batch
                    self._broadcast_flush()
                else:
                    print(f"[COORDINATOR] Received batch {batch.id()} without last_batch flag. Ignoring.")

    def _broadcast_flush(self):
        for _, producer in self.producer_queues.items():
            producer.send(FLUSH_MESSAGE)
        print("[Coordinator] Sent FLUSH to all nodes")
        
    def _send_downstream_end(self):
        self.downstream_queue.send(self.end_batch.encode())
        print("[Coordinator] Sent END_BATCH downstream\n")
        print(f"\n el batch es:\n {self.end_batch} \n\n")

        print(f"\n el batch es:\n {self.end_batch} \n\n")

    def close(self):
        self.consumer_queue.stop_consuming()
        self.consumer_queue.close()
        print("[Coordinator] consumer queue closed.")
        for q in list(self.producer_queues.values()):
            q.close()