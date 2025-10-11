import os
import socket
import threading
import logging
import sys
import signal
from collections import defaultdict
from common.protocol import send_batches_from_csv, recv_batch, recv_client_id
import time

from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

BATCH_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])
AMOUNT_OF_QUERIES = 5

STORES_PATH = '/stores'
TRANSACTION_PATH = '/transactions'
TRANSACTION_ITEMS_PATH = '/transaction_items'
USERS_PATH = '/users'
MENU_ITEM_PATH = '/menu_items'

STORES_TYPE_FILE = 's'
TRANSACTION_TYPE_FILE = 't'
TRANSACTION_ITEMS_TYPE_FILE = 'i'
USERS_TYPE_FILE = 'u'
MENU_ITEM_TYPE_FILE = 'm'


class Client:

    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        # self.sender_transaction = None
        self.receiver_thread = None
        self.client_id = -1

        self.shutdown_event = threading.Event()

        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        self.shutdown_event.set()
        try:
            logging.info("[CLIENT] Recibida señal SIGTERM, cerrando...")
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
            self.close()
            logging.info("[CLIENT] Client cerrado correctamente.")
        except Exception as e:
            logging.error(f"[CLIENT] Error al cerrar: {e}")
        sys.exit(0)

    def start(self, path_input, path_output):

        self.client_id = recv_client_id(self.socket)

        send_batches_from_csv(path_input+STORES_PATH, BATCH_SIZE, self.socket, STORES_TYPE_FILE, self.client_id)

        #send_batches_from_csv(path_input+USERS_PATH, BATCH_SIZE, self.socket, USERS_TYPE_FILE, self.client_id)

        send_batches_from_csv(path_input+MENU_ITEM_PATH, BATCH_SIZE, self.socket, MENU_ITEM_TYPE_FILE, self.client_id)

        send_batches_from_csv(path_input+TRANSACTION_ITEMS_PATH, BATCH_SIZE, self.socket, TRANSACTION_ITEMS_TYPE_FILE, self.client_id)
        
        #send_batches_from_csv(path_input + TRANSACTION_PATH, BATCH_SIZE, self.socket, TRANSACTION_TYPE_FILE, self.client_id)

        self.sender_transaction = threading.Thread(
            target=send_batches_from_csv,
            args=(path_input + TRANSACTION_PATH, BATCH_SIZE, self.socket, TRANSACTION_TYPE_FILE, self.client_id),
            daemon=True
        )
        self.sender_transaction.start()

        self.receiver_thread = threading.Thread(
            target=self.receiver, args=(path_output,), daemon=True
        )
        self.receiver_thread.start()
        logging.debug(f"[CLIENT] Hilos sender y receiver iniciados.")

    def close(self):
        if self.receiver_thread:
            self.receiver_thread.join()
            logging.debug("[CLIENT] joinee receiver...")
        # if self.sender_transaction:
        #     self.sender_transaction.join()
            logging.debug("[CLIENT] joinee sender transactions...")
        self.socket.close()
        logging.info("[CLIENT] socket cerrado.")

    def receiver(self, out_dir):
        files = {}
        wrote_header = {}
        ended = set()

        expected_batches = {}
        received_batches = defaultdict(int)

        os.makedirs(out_dir, exist_ok=True)
        try:
            while not self.shutdown_event.is_set():
                batch = recv_batch(self.socket)
                # print(f"recibi batch {batch.id()}")
                qid = batch.get_query_id()
                if batch.client_id() != self.client_id:
                    logging.info("[CLIENT] llego un batch con client_id distinto")
                    continue
                    
                if batch.is_last_batch():
                    idx = batch.get_header().index('cant_batches')
                    expected_batches[qid] = int(batch[0][idx])
                    logging.debug(f"[CLIENT] Q{qid} espera {expected_batches[qid]} batches.")

                    if qid in expected_batches and received_batches[qid] == expected_batches[qid]:
                        ended.add(qid)
                        logging.debug(f"[CLIENT] Recibido END de Q{qid}. Pendientes: {AMOUNT_OF_QUERIES - len(ended)}")
                        if len(ended) >= AMOUNT_OF_QUERIES:
                            logging.info("[CLIENT] Recibidos END de todas las queries. Fin.")
                            break
                    continue

                received_batches[qid] += 1
                
                if qid not in files:
                    path = os.path.join(out_dir, f"q{qid}.csv")
                    files[qid] = open(path, "w")
                    wrote_header[qid] = False
                    logging.debug(f"[CLIENT] Abierto archivo de salida: {path}")

                f = files[qid]
                if not wrote_header[qid]:
                    f.write(",".join(batch.get_header()) + "\n")
                    wrote_header[qid] = True
                    logging.debug(f"[CLIENT] Escribí header para Q{qid}: {batch.get_header()}")

                for row in batch:
                    f.write(",".join(row) + "\n")

                if qid in expected_batches and received_batches[qid] == expected_batches[qid]:
                    ended.add(qid)
                    logging.debug(f"[CLIENT] Recibido END de Q{qid}. Pendientes: {AMOUNT_OF_QUERIES - len(ended)}")
                    if len(ended) >= AMOUNT_OF_QUERIES:
                        logging.info("[CLIENT] Recibidos END de todas las queries. Fin.")
                        break

        except Exception as e:
            logging.info(f"\n[CLIENT] Error en receiver: {e}")
        finally:
            for f in files.values():
                try:
                    f.close()
                except Exception:
                    pass
            logging.info(f"[CLIENT] Resultados guardados en {out_dir}")
