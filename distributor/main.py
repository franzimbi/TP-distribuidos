#!/usr/bin/env python3
import os
import socket
import threading
from common.protocol import recv_batch
from distributor import Distributor, shutdown
from configparser import ConfigParser
import logging

def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SYSTEM_PORT', config["DEFAULT"]["SYSTEM_PORT"]))
        config_params["host"] = str(os.getenv('SYSTEM_HOST', config["DEFAULT"]["SYSTEM_HOST"]))
        config_params["listen_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SYSTEM_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger("pika").setLevel(logging.WARNING)

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

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()
    server_socket.settimeout(1.0)

    def handle_client(sock, addr):
        client_id = 1  # todo: mover a accept
        distributor.add_client(client_id, sock)
        try:
            query_id = None
            while True:
                batch = recv_batch(sock)
                if query_id is None:
                    query_id = batch.get_query_id()
                    logging.debug(f"[DISTRIBUTOR] Cliente {addr} inició con query_id {query_id}")
                distributor.distribute_batch_to_workers(query_id, batch)
                if batch.is_last_batch():
                    print(f"[DISTRIBUTOR] Cliente {addr} terminó de ENVIAR (esperando resultados de workers).")
                    break
        except Exception as e:
            print(f"Error con cliente {addr}: {e}")
            try:
                sock.close()
            except OSError as e:
                pass
            distributor.remove_client(client_id)
            print(f"Cliente desconectado (por error): {addr}")

    def accept_clients():
        threads = []
        while not shutdown.is_set():
            threads = [t for t in threads if t.is_alive()]
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

    # q3_consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(3,), daemon=True)
    # q3_consumer_thread.start()

    accept_thread = threading.Thread(target=accept_clients, daemon=True)
    accept_thread.start()

    try:
        while True:
            accept_thread.join(timeout=1.0)
            q1_consumer_thread.join(timeout=1.0)
            # q3_consumer_thread.join(timeout=1.0)
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
        # q3_consumer_thread.join(timeout=2.0)
        print("[DISTRIBUTOR] Apagado limpio.")


if __name__ == "__main__":
    main()
