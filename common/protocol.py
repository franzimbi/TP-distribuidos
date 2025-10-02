from middleware.middleware import MessageMiddlewareQueue
import os
import socket
from common.batch import Batch
import logging

def send_batches_from_csv(path, batch_size, connection: socket, type_file, query_id):
    print(f"[PROTOCOL] Enviando batches desde {path} de tipo {type_file} para query {query_id}")
    current_batch = Batch(type_file=type_file)
    for filename in os.listdir(path):
        if filename.endswith('.csv'):    
            with open(path + '/' + filename, 'r', encoding='utf-8', errors='replace') as f:
                #printear el nombre del archivo que se esta leyendo
                # logging.info(f"[PROTOCOL] Leyendo archivo: {filename}")
                headers = next(f)
                try:
                    current_batch.set_header(headers)
                except RuntimeError as e:
                    logging.debug("[PROTOCOL] error set_header: %s", e)
                for line in f:
                    current_batch.add_row(line)
                    if len(current_batch) >= batch_size:
                        current_batch.set_query_id(query_id) # no se deberia hacer aca, sino en el distributor, yo lo cambi desp ~pedro
                        send_batch(connection, current_batch)
                        current_batch.reset_body_and_increment_id()
    if len(current_batch) > 0:
        current_batch.set_query_id(query_id)
        send_batch(connection, current_batch)
        current_batch.reset_body_and_increment_id()
    current_batch.set_last_batch(True)
    current_batch.set_query_id(query_id)
    send_batch(connection, current_batch)


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

