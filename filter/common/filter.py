from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import logging
import signal
import sys
import threading

BUFFER_SIZE = 150
FLUSH_MESSAGE = 'FLUSH'
END_MESSAGE = 'END'

class Buffer:
    def __init__(self):
        pass


class Filter:
    def __init__(self, consume_queue, produce_queue, filter, coordinator_consumer, coordinator_producer):
        self._buffer = Batch()
        self._counter = 0
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)
        
        self._coordinator_consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_consumer)
        self._coordinator_produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_producer)
        
        self._filter = filter
        self.conection_coordinator = None
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        self.lock = threading.Lock()

    def graceful_shutdown(self, signum, frame):
        try:
            self.close()
        except Exception as e:
            logging.error(f"[FILTER] Error al cerrar: {e}")
        sys.exit(0)

    def start(self):
        self.conection_coordinator = threading.Thread(
            target=self._coordinator_consume_queue.start_consuming, 
            args=(self.coordinator_callback,), daemon=True
        )
        self.conection_coordinator.start()
        self._consume_queue.start_consuming(self.callback)


    def callback(self, ch, method, properties, message):
        with self.lock:
            batch = Batch()
            batch.decode(message)

            try:
                result = self._filter(batch)
                if not result.is_empty():
                    if self._buffer.is_empty() and result.get_header():
                        self._buffer.set_header(result.get_header())
                        self._buffer.set_query_id(batch.get_query_id())
                    for i in result:
                        self._buffer.add_row(i)
            except Exception as e:
                logging.error(f"\n\n\n error: {e} | batch:{batch} | filter:{self._filter.__name__}")

            if batch.is_last_batch():
                self._buffer.set_last_batch()
                self._buffer.set_id(batch.id())
                self._buffer.set_query_id(batch.get_query_id())
                self._buffer.set_header(result.get_header()) #setteamos header por las dudas
                self._coordinator_produce_queue.send(self._buffer.encode())
                self._buffer = Batch(type_file=batch.type())
                # self.received_end = True

            if len(self._buffer) >= BUFFER_SIZE:
                self._buffer.set_id(batch.id())
                self._buffer.set_query_id(batch.get_query_id())
                self._produce_queue.send(self._buffer.encode())
                self._buffer = Batch(type_file=batch.type())


    def coordinator_callback(self, ch, method, properties, body):
        msg = body.decode('utf-8')
        if str(msg) == str(FLUSH_MESSAGE):
            with self.lock:
                if not self._buffer.is_empty():
                    self._produce_queue.send(self._buffer.encode())
                    self._buffer = Batch()
                    self._coordinator_produce_queue.send(END_MESSAGE)
                else:
                    self._coordinator_produce_queue.send(END_MESSAGE)
        else:
            logging.error(f"[FILTER] Unknown command from coordinator: {msg}")
                    
    def close(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        self._produce_queue.close()
        self._coordinator_consume_queue.close()
        self._coordinator_consume_queue.stop_consuming()
        self._coordinator_produce_queue.close()
