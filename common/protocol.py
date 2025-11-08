from middleware.middleware import MessageMiddlewareQueue
import os
import socket
from common.batch import Batch
import logging


def send_batches_from_csv(path, batch_size, connection: socket, type_file, client_id):
    logging.debug(f"[PROTOCOL] Enviando batches desde {path} de tipo {type_file} para client {client_id}")
    current_batch = Batch(client_id=client_id,type_file=type_file)
    cant_batches = 0
    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            with open(path + '/' + filename, 'r', encoding='utf-8', errors='replace') as f:
                headers = next(f)
                try:
                    current_batch.set_header(headers)
                except RuntimeError as e:
                    logging.debug("[PROTOCOL] error set_header: %s", e)
                for line in f:
                    current_batch.add_row(line)
                    if len(current_batch) >= batch_size:
                        send_batch(connection, current_batch)
                        current_batch.reset_body_and_increment_id()
                        cant_batches += 1
    if len(current_batch) > 0:
        send_batch(connection, current_batch)
        # current_batch.reset_body_and_increment_id()
        cant_batches += 1

    current_batch.reset_body_and_increment_id()
    current_batch.set_last_batch(True)
    current_batch.set_header(['cant_batches'])
    current_batch.add_row(str(cant_batches))
    send_batch(connection, current_batch)

def recv_client_id(socket):
    size_bytes = recv_exact(socket, 4)
    id = int.from_bytes(size_bytes, "big")
    return id


def send_batch(socket, batch: Batch):
    data = batch.encode()
    size = len(data)
    socket.sendall(size.to_bytes(4, "big"))
    socket.sendall(data)


def recv_exact(socket, n):
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


def send_joins_confirmation_to_client(socket):
    confirmation = 1
    socket.sendall(confirmation.to_bytes(1, "big"))

def recv_joins_confirmation_from_distributor(socket):
    size_bytes = recv_exact(socket, 1)
    confirmation = int.from_bytes(size_bytes, "big")
    return confirmation