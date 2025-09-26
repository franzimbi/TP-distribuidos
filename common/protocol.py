from middleware.middleware import MessageMiddlewareQueue
import os
import socket
from common.batch import Batch

def send_batches_from_csv(path, batch_size, connection: socket, type_file):
    current_batch = Batch(type_file=type_file)
    for filename in os.listdir(path):

        with open(path + '/' + filename, 'r') as f:
            headers = next(f)
            try:
                current_batch.set_header(headers)
            except RuntimeError as e:
               print("[PROTOCOL] error al setear headers | error: {}".format(e))
            for line in f:
                current_batch.add_row(line)
                if len(current_batch) >= batch_size:
                    send_batch(connection, current_batch)
                    current_batch.reset_body_and_increment_id()

    if len(current_batch) > 0:
        send_batch(connection, current_batch)
        current_batch.reset_body_and_increment_id()

    current_batch.set_last_batch(True)
    send_batch(connection, current_batch)


def send_batch(socket, batch: Batch):
    data = batch.encode()
    size = len(data)
    socket.sendall(size.to_bytes(4, "big"))
    socket.sendall(data)


def recv_exact(socket, n):
    """Recibe exactamente n bytes del socket."""
    buffer = b''
    while len(buffer) < n:
        chunk = socket.recv(n - len(buffer))
        if not chunk:
            raise ConnectionError("Socket cerrado antes de tiempo")
        buffer += chunk
    return buffer


def recv_batch(socket):
    size_bytes = recv_exact(socket, 4)
    size = int.from_bytes(size_bytes, "big")
    data = recv_exact(socket, size)

    batch = Batch()
    batch.decode(data)
    return batch


def recv_batches_from_socket(connection: socket):
    return recv_batch(connection)
    # buffer = b''
    # while True:
    #     data = connection.recv(4096)
    #     if not data:
    #         break
    #     buffer += data
    #
    #     while True:
    #         if len(buffer) < 13:
    #             break
    #
    #         header_len = int.from_bytes(buffer[6:10], byteorder='big', signed=False)
    #         if len(buffer) < 10 + header_len + 4:
    #             break  # no hay suficiente data para leer header_len
    #
    #         size_offset = 10 + header_len
    #         if len(buffer) < size_offset + 4:
    #             break  # no hay suficiente data para leer size
    #
    #         body_len_offset = size_offset + 4
    #         if len(buffer) < body_len_offset + 4:
    #             break  # no hay suficiente data para leer body_len
    #
    #         body_len = int.from_bytes(buffer[body_len_offset:body_len_offset + 4], byteorder='big', signed=False)
    #         total_batch_size = body_len_offset + 4 + body_len
    #         if len(buffer) < total_batch_size:
    #             break  # no hay suficiente data para leer todo el batch
    #
    #         batch_data = buffer[:total_batch_size]
    #         batch = Batch(id=0)  # id temporal, despues se setea bien en decode
    #         batch.decode(batch_data)
    #
    #         yield batch
    #
    #         buffer = buffer[total_batch_size:]
    #
    #         if batch.is_last_batch():
    #             return

