from middleware.middleware import MessageMiddlewareQueue
from common.protocol import decode_batch
from common.batch import Batch

BUFFER_SIZE = 150
END_MARKER = b"&END&"


class Filter:
    def __init__(self, consume_queue, produce_queue, filter):
        self._buffer = ''
        self._counter = 0
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)
        self._filter = filter

    def start(self):
        self._consume_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        if message == END_MARKER:
            print("[FILTER] Marcador END recibido, finalizando.")
            self.send_current_buffer()
            self._produce_queue.send(END_MARKER)
            return

        #batch = decode_batch(message) #viejo
        batch = Batch(type_file='t')
        batch.decode(message)
        result = self._filter(batch)

        if not result:
            print("[FILTER] Resultado vacío, no se envía nada.")
            return

        self._buffer = result.encode()
        self._produce_queue.send(self._buffer)
        print(f"[FILTER] Batch procesado enviado.")
        self._buffer = ""

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
