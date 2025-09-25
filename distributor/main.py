#!/usr/bin/env python3
import os
import socket
import threading
from common.protocol import recv_batches_from_socket
from distributor import Distributor, shutdown

distributor = Distributor()

TCP_PORT = int(os.getenv("TCP_PORT", 5000))
HOST = "0.0.0.0"
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, TCP_PORT))
server_socket.listen()
server_socket.settimeout(1.0)

print(f"Distributor escuchando en {HOST}:{TCP_PORT}")

def handle_client(sock, addr):
    print(f"Cliente conectado: {addr}")
    client_id = 1
    distributor.add_client(client_id, sock)

    try:
        query_id = 1
        for batch in recv_batches_from_socket(sock):
            distributor.distribute_batch_to_workers(query_id, batch)
            if batch.is_last_batch():
                print(f"[DISTRIBUTOR] Cliente {addr} terminó de ENVIAR (esperando resultados de workers).")
                break
    except Exception as e:
        print(f"Error con cliente {addr}: {e}")
        try:
            sock.close()
        except Exception:
            pass
        if distributor.clients.get(client_id) is sock:
            del distributor.clients[client_id]
        print(f"Cliente desconectado (por error): {addr}")

def accept_clients():
    threads = []
    while not shutdown.is_set():
        try:
            sock, addr = server_socket.accept()
        except socket.timeout:
            continue
        t = threading.Thread(target=handle_client, args=(sock, addr), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=1.0)

q1_consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(1,), daemon=True)
q1_consumer_thread.start()

accept_thread = threading.Thread(target=accept_clients, daemon=True)
accept_thread.start()

try:
    while True:
        accept_thread.join(timeout=1.0)
        q1_consumer_thread.join(timeout=1.0)
except KeyboardInterrupt:
    pass
finally:
    shutdown.set()
    distributor.stop_consuming_from_all_workers()
    try:
        server_socket.close()
    except Exception:
        pass
    accept_thread.join(timeout=2.0)
    q1_consumer_thread.join(timeout=2.0)
    print("[DISTRIBUTOR] Apagado limpio.")
