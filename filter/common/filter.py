from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch

BUFFER_SIZE = 150


class Filter:
    def __init__(self, consume_queue, produce_queue, filter):
        self._buffer = Batch()
        self._counter = 0
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)
        self._filter = filter

    def start(self):
        self._consume_queue.start_consuming(self.callback)

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
            if batch.is_last_batch():
                self._buffer.set_last_batch()
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
