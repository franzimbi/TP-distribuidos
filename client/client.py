import os
import socket
import threading
import logging
import sys
import signal
from common.protocol import send_batches_from_csv, recv_batch

BATCH_SIZE = 150
AMOUNT_OF_QUERIES = 2

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
        self.sender_transaction = None
        self.receiver_thread = None

        self.shutdown_event = threading.Event()

        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        self.shutdown_event.set()
        try:
            print("Recibida señal SIGTERM, cerrando client...")
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
            self.close()
            print("Client cerrado correctamente.")
        except Exception as e:
            logging.error(f"[CLIENT] Error al cerrar: {e}")
        sys.exit(0)

    def start(self, path_input, path_output):
    
        # send_batches_from_csv(path_input+STORES_PATH, BATCH_SIZE, self.socket, STORES_TYPE_FILE, 3)

        # send_batches_from_csv(path_input+USERS_PATH, BATCH_SIZE, self.socket, USERS_TYPE_FILE, 4)

        send_batches_from_csv(path_input+MENU_ITEM_PATH, BATCH_SIZE, self.socket, MENU_ITEM_TYPE_FILE, 21)

        self.sender_transaction_items = threading.Thread(
            target=send_batches_from_csv,
            args=(path_input+TRANSACTION_ITEMS_PATH, BATCH_SIZE, self.socket, TRANSACTION_ITEMS_TYPE_FILE, 21),
            daemon=True
        )
        self.sender_transaction_items.start()

        # self.sender_transaction = threading.Thread(
        #     target=send_batches_from_csv,
        #     args=(path_input+TRANSACTION_PATH, BATCH_SIZE, self.socket, TRANSACTION_TYPE_FILE, 1),
        #     daemon=True
        # )
        # self.sender_transaction.start()
        
        self.receiver_thread = threading.Thread(
            target=self.receiver, args=(path_output,), daemon=True
        )
        self.receiver_thread.start()
        logging.debug(f"[CLIENT] Hilos sender y receiver iniciados.")

    def close(self):
        if self.receiver_thread:
            self.receiver_thread.join()
            logging.debug("[CLIENT] joinee receiver...")
        if self.sender_transaction:
            self.sender_transaction.join()
            logging.debug("[CLIENT] joinee sender transactions...")
        self.socket.close()
        logging.info("[CLIENT] socket cerrado.")


    def receiver(self, out_dir):
        files = {}
        wrote_header = {}
        ended = set()
        os.makedirs(out_dir, exist_ok=True)
        try:
            while not self.shutdown_event.is_set():
                batch = recv_batch(self.socket)
                qid = batch.get_query_id()

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
                
                if batch.is_last_batch():
                    ended.add(qid)
                    logging.debug(f"[CLIENT] Recibido END de Q{qid}. Pendientes: {AMOUNT_OF_QUERIES - len(ended)}")
                    if len(ended) >= AMOUNT_OF_QUERIES:
                        logging.info("[CLIENT] Recibidos END de todas las queries. Fin.")
                        break
                    continue

        except Exception as e:
            logging.info(f"\n[CLIENT] Error en receiver: {e}")
        finally:
            for f in files.values():
                try:
                    f.close()
                except Exception:
                    pass
            logging.info(f"[CLIENT] Resultados guardados en {out_dir}")
