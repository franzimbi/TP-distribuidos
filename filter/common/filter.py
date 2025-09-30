from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import logging
import signal
import sys

BUFFER_SIZE = 150


class Filter:
    def __init__(self, consume_queue, produce_queue, filter):
        self._buffer = Batch()
        self._counter = 0
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)
        self._filter = filter

        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        try:
            self.close()
        except Exception as e:
            logging.error(f"[FILTER] Error al cerrar: {e}")
        sys.exit(0)

    def start(self):
        self._consume_queue.start_consuming(self.callback)

    # def coordinator_callback(self, ch, method, properties, body):
    #     msg = body.decode()
    #     if msg == "FLUSH":
    #         print("[FILTER] Received FLUSH command from coordinator.")
    #
    #         to_send = None
    #         with self._buffer_lock:
    #             if not self._buffer.is_empty() and not self.received_end:
    #                 to_send = self._buffer
    #                 self._buffer = Batch(type_file=self._buffer.type())
    #
    #         # envío fuera del lock
    #         if to_send:
    #             self._produce_queue.send(to_send.encode())
    #
    #         self._coordinator_produce_queue.send(b"NODE_FINISHED")
    #         print("[FILTER] Sent NODE_FINISHED to coordinator.")
    #     else:
    #         print(f"[FILTER] Unknown command from coordinator: {msg}")

    # def callback(self, ch, method, properties, message):
    #     # agrego filas filtradas al buffer
    #     with self._buffer_lock:
    #         batch = Batch()
    #         batch.decode(message)

    #         result = self._filter(batch)

    #         if batch.is_last_batch():
    #             print("[FILTER] Received LAST batch")

    #         if not result.is_empty():
    #             if self._buffer.is_empty() and result.get_header():
    #                 self._buffer.set_header(result.get_header())
    #             for i in result:
    #                 self._buffer.add_row(i)

    #         to_send = None
    #         send_to_coordinator = False

    #         if len(self._buffer) >= BUFFER_SIZE or batch.is_last_batch():
    #             self._buffer.set_id(batch.id())
    #             self._buffer.set_query_id(batch.get_query_id())
    #             if batch.is_last_batch():
    #                 self._buffer.set_last_batch()
    #                 self.received_end = True
    #                 to_send = self._buffer
    #                 send_to_coordinator = True
    #                 self._buffer = Batch(type_file=batch.type())
    #             else:
    #                 to_send = self._buffer
    #                 self._buffer = Batch(type_file=batch.type())

    
        # # envío fuera del lock
        # if to_send:
        #     if send_to_coordinator:
        #         self._coordinator_produce_queue.send(to_send.encode())
        #         print("[FILTER] Sent LAST batch to coordinator.")
        #     else:
        #         self._produce_queue.send(to_send.encode())

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)

        try:
            result = self._filter(batch)
            if not result.is_empty():
                if self._buffer.is_empty() and result.get_header():
                    self._buffer.set_header(result.get_header())
                for i in result:
                    self._buffer.add_row(i)
        except Exception as e:
            print(f"\n\n\n error: {e} | batch:{batch} | filter:{self._filter.__name__}")

        if len(self._buffer) >= BUFFER_SIZE or batch.is_last_batch():
            self._buffer.set_id(batch.id())
            self._buffer.set_query_id(batch.get_query_id())
            if batch.is_last_batch():
                self._buffer.set_last_batch()
            self._produce_queue.send(self._buffer.encode())
            self._buffer = Batch(type_file=batch.type())

    def send_current_buffer(self):
        if self._counter > 0:
            self._produce_queue.send(self._buffer)
            self._buffer = ""
            self._counter = 0

    def close(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        self._produce_queue.close()
