from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
import threading

BUFFER_SIZE = 150


class Filter:
    def __init__(self, consume_queue, produce_queue, filter, coordinator_consume_queue, coordinator_produce_queue):
        self._buffer_lock = threading.Lock()
        self._buffer = Batch()
        self._counter = 0
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)

        self._coordinator_consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_consume_queue)
        self._coordinator_produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=coordinator_produce_queue)
        
        self._filter = filter
        self.received_end = False
        self.threads = []

    def start(self):
        t = threading.Thread(
            target=self._coordinator_consume_queue.start_consuming, 
            args=(self.coordinator_callback,), daemon=True
        )       
        self.threads.append(t)
        t.start()
        self._consume_queue.start_consuming(self.callback)

    def coordinator_callback(self, ch, method, properties, body):
        msg = body.decode()
        if msg == "FLUSH":
            with self._buffer_lock:
                print("[FILTER] Received FLUSH command from coordinator.")
                if not self._buffer.is_empty() and not self.received_end:
                    self._produce_queue.send(self._buffer.encode()) #ojo, en multi cliente aca deberia usar un lock
                    self._buffer = Batch(type_file=self._buffer.type())
                self._coordinator_produce_queue.send("NODE_FINISHED")
                print("[FILTER] Sent NODE_FINISHED to coordinator.")
        else:
            print(f"[FILTER] Unknown command from coordinator: {msg}")

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)

        result = self._filter(batch)
        if batch.is_last_batch():
            print("aca hay last batch")
        if not result.is_empty():
            with self._buffer_lock:      
                if self._buffer.is_empty() and result.get_header():
                    self._buffer.set_header(result.get_header())          
                for i in result:
                    self._buffer.add_row(i)
        
                if len(self._buffer) >= BUFFER_SIZE or batch.is_last_batch():
                    self._buffer.set_id(batch.id())
                    self._buffer.set_query_id(batch.get_query_id())
                    if batch.is_last_batch():
                        print("\n\n[FILTER] Received last batch, sending remaining data and notifying coordinator.\n\n")
                        self._buffer.set_last_batch()
                        self._coordinator_produce_queue.send(self._buffer.encode())
                        print("\n\nFILTER] last batch to coordinator.\n\n")
                        self._buffer = Batch(type_file=batch.type())
                        self.received_end = True
                    else:
                        self._produce_queue.send(self._buffer.encode())
                        self._buffer = Batch(type_file=batch.type())

        # batch = message.decode()
        # if batch.is_last_batch():
        #     print("[FILTER] Marcador END recibido, finalizando.")
        #     self.send_current_buffer()
        #     self._produce_queue.send(END_MARKER)
        #     return
        #
        # #batch = decode_batch(message) #viejo
        # batch = Batch(type_file='t')
        # batch.decode(message)
        # result = self._filter(batch)
        #
        # if not result:
        #     print("[FILTER] Resultado vacío, no se envía nada.")
        #     return
        #
        # self._buffer = result.encode()
        # self._produce_queue.send(self._buffer)
        # print(f"[FILTER] Batch procesado enviado.")
        # self._buffer = ""

        # for row in result:
        #     line = ",".join(row)
        #     self._buffer = (self._buffer + "|" + line) if self._buffer else line
        #     self._counter += 1
        #     if self._counter >= BUFFER_SIZE:
        #         self.send_current_buffer()

    def send_current_buffer(self):
        # print(f"\n\nMANDO BUFFER: {self._buffer}\n")
        if self._counter > 0:
            self._produce_queue.send(self._buffer)
            self._buffer = ""
            self._counter = 0
