from middleware.middleware import MessageMiddlewareQueue
from common.protocol import decode_batch

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
            self.send_current_buffer()
            self._produce_queue.send(END_MARKER)
            return

        batch = decode_batch(message)
        result = self._filter(batch)

        if not result:
            return

        for row in result:
            line = ",".join(row)
            self._buffer = (self._buffer + "|" + line) if self._buffer else line
            self._counter += 1
            if self._counter >= BUFFER_SIZE:
                self.send_current_buffer()

    def send_current_buffer(self):
        # print(f"\n\nMANDO BUFFER: {self._buffer}\n")
        if self._counter > 0:
            self._produce_queue.send(self._buffer)
            self._buffer = ""
            self._counter = 0
