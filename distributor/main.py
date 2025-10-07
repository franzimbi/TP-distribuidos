#!/usr/bin/env python3
import os
import socket
import threading
from common.protocol import recv_batch
from distributor import Distributor
from configparser import ConfigParser
import logging
import signal


def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SYSTEM_PORT', config["DEFAULT"]["SYSTEM_PORT"]))
        config_params["host"] = str(os.getenv('SYSTEM_HOST', config["DEFAULT"]["SYSTEM_HOST"]))

        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SYSTEM_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger("pika").setLevel(logging.CRITICAL)


def send_id_to_client(client_id, socket):
    socket.sendall(client_id.to_bytes(4, "big"))


def handle_client(socket, shutdown, distributor):
    print(f"[DISTRIBUTOR] Iniciando manejo de cliente...")
    counter_lasts_batches = 0
    while not shutdown.is_set():
        batch = recv_batch(socket)
        if batch:
            if batch.is_last_batch():
                counter_lasts_batches += 1
                print(f"[DISTRIBUTOR] Recibido batch final {batch.id()} de tipo {batch.type()} de client{batch.client_id()}.")
            if batch.id() % 20000 == 0 or batch.id() == 0:
                print(f"[DISTRIBUTOR] Recibido batch {batch.id()} de tipo {batch.type()} de client{batch.client_id()}.")
            distributor.distribute_batch_to_workers(batch)
        if counter_lasts_batches >= 5:
            return


def graceful_quit(signum, frame, shutdown, server_socket, distributor, client_threads):
    logging.debug("Recibida señal SIGTERM, cerrando distributor...")
    shutdown.set()
    try:
        server_socket.shutdown(socket.SHUT_RDWR)
        server_socket.close()
    except OSError as e:
        logging.debug("OS error: {}".format(e))
    distributor.stop_consuming_from_all_workers()
    for i in client_threads:
        i.join()


def main():
    config_params = initialize_config()
    distributor = Distributor()

    port = config_params["port"]
    host = config_params["host"]
    logging_level = config_params["logging_level"]
    listen_backlog = config_params["listen_backlog"]
    initialize_log(logging_level)
    logging.debug(f"action: config | result: success | port: {port} | host: {host}  | "
                  f"listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    client_threads = []
    shutdown = threading.Event()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()

    signal.signal(signal.SIGTERM,
                  lambda signum, frame: graceful_quit(signum, frame, shutdown, server_socket, distributor, client_threads))
    distributor.start_consuming_from_workers()

    while True:
        client_threads = [t for t in client_threads if t.is_alive()]
        print(f"\n[DISTRIBUTOR] Esperando conexiones de clientes en {host}:{port}...\n")
        client_socket, client_address = server_socket.accept()
        print(f"[DISTRIBUTOR] Cliente conectado desde {client_address}")
        id_client = distributor.add_client(client_socket)
        send_id_to_client(id_client, client_socket)
        new_client = threading.Thread(target=handle_client, args=(client_socket, shutdown, distributor),
                                      daemon=True)
        client_threads.append(new_client)
        new_client.start()


if __name__ == "__main__":
    main()
