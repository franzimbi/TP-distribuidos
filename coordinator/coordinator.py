import os
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import logging
import sys
import signal

FLUSH_MESSAGE = 'FLUSH'
END_MESSAGE = 'END'


class Coordinator:
    def __init__(self, num_nodes, consumer, producers, downstream):
        self.num_nodes = num_nodes
        self.consumer_queue = MessageMiddlewareQueue(host='rabbitmq', queue_name=consumer)
        self.downstream_queue = MessageMiddlewareQueue(host='rabbitmq', queue_name=downstream)
        self.producer_queues = {}

        for i in range(len(producers)):
            self.producer_queues[i + 1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=producers[i])

        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        self.end_batch = None
        self.finished_nodes = 0

    def graceful_shutdown(self, signum, frame):
        try:
            self.close()
        except Exception as e:
            logging.error(f"[Coordinator] Error during shutdown: {e}")
        sys.exit(0)

    def start(self):
        self.consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            msg = body.decode('utf-8')
            if len(body) <= 4:
                if msg == END_MESSAGE:
                    print(f"\n[Coordinator] llego {msg}")
                    self.finished_nodes += 1
                    if self.finished_nodes == self.num_nodes:
                        print(f"\n[Coordinator] finished {self.finished_nodes}/{self.num_nodes}")
                        self._send_downstream_end()
                        self.end_batch = None
                        self.finished_nodes = 0
                        return
                else:
                    logging.error(f"[Coordinator] Unknown text message: {msg}")
                return
        except UnicodeDecodeError as e:
            print(f"\n\n[Coordinator] llego end batch")
            batch = Batch()
            batch.decode(body)
            if batch.is_last_batch():
                self.end_batch = batch
                self._broadcast_flush()
            else:
                logging.error(f"[COORDINATOR] Received batch {batch.id()} without last_batch flag. Ignoring.")

    def _broadcast_flush(self):
        for _, producer in self.producer_queues.items():
            producer.send(FLUSH_MESSAGE)

    def _send_downstream_end(self):
        self.downstream_queue.send(self.end_batch.encode())
        print(f'[Coordinator] \n\nmande end batch')

    def close(self):
        self.consumer_queue.stop_consuming()
        self.consumer_queue.close()
        for q in list(self.producer_queues.values()):
            q.close()
        self.downstream_queue.close()
