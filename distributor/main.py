#!/usr/bin/env python3
import os
import socket
import threading
from common.protocol import recv_batch
from distributor import Distributor, shutdown
from configparser import ConfigParser
import logging


def initialize_config():
    config = ConfigParser(os.environ)
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
        client_id = 1  # TODO: IDs únicos si soportan varios clientes
        distributor.add_client(client_id, sock)
        contador = 0 
        first_batch_type = None

        try:
            while True:
                batch = recv_batch(sock)

                if first_batch_type is None:
                    first_batch_type = batch.type()
                    # distributor.set_client_queries_for_type(client_id, first_batch_type)
                    logging.debug(f"[DISTRIBUTOR] Cliente {addr} inició tipo={first_batch_type}")

                distributor.distribute_batch_to_workers(batch)

                if batch.is_last_batch():
                    contador += 1
                    print(f"\n[DISTRIBUTOR] Cliente {addr} terminó de ENVIAR {batch.type()}(esperando resultados de workers).\n")
                    if contador == 2: #pq por ahora solo hacemos 2 queries
                        print(f"\n[DISTRIBUTOR] Cliente {addr} terminó de ENVIAR todos los tipos de archivos.\n")
                        # break #mejor sacar este break para que cuando el cliente cierre la conexion, el except pueda cerrar todo
                    continue

        except Exception as e:
            print(f"cliente {addr}: cerro la conexion")
            try:
                sock.close()
            except OSError:
                print("Socket ya cerrado")
                pass
            distributor.remove_client(client_id)

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
            for t in threads: #desp d aceptar un cliente, me aseguro d limpiar los threads muertos
                t.join(timeout=1.0)
                if not t.is_alive():
                    threads.remove(t) 
        for t in threads:
            t.join()

    q1_consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(1,), daemon=True)
    q1_consumer_thread.start()
    
    # q3_consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(3,), daemon=True)
    # q3_consumer_thread.start()

    # q4_consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(4,), daemon=True)
    # q4_consumer_thread.start()

    accept_thread = threading.Thread(target=accept_clients, daemon=True)
    accept_thread.start()

    try:
        while True:
            accept_thread.join() # no agrueguen timeout aca, crean un busy loop alpedo
    except KeyboardInterrupt:
        pass
    finally:
        distributor.stop_consuming_from_all_workers()
        print("\nhice stop consuming from all threads")
        try:
            server_socket.close()
        except Exception:
            pass
        accept_thread.join(timeout=2.0)
        print("joinee accept thread")
        q1_consumer_thread.join(timeout=2.0)
        print("joinee q1_consumer thread")
        # q3_consumer_thread.join(timeout=2.0)
        # print("joinee q3_consumer thread")
        # q4_consumer_thread.join()
        # print("joinee q4_consumer thread")

        print("[DISTRIBUTOR] Apagado limpio.")

if __name__ == "__main__":
    main()
