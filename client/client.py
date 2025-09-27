import os
import socket
import threading
import logging
from common.protocol import send_batches_from_csv, recv_batch

BATCH_SIZE = 150
AMOUNT_OF_QUERIES = 2

class Client:

    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))

    def start(self, path_input, path_output):
        self.sender = threading.Thread(
            target=send_batches_from_csv,
            args=(path_input, BATCH_SIZE, self.socket, 't', 1),
            daemon=True
        )
        self.sender.start()

        self.receiver = threading.Thread(
            target=receiver, args=(self.socket, 'results'), daemon=True
        )
        self.receiver.start()

    def close(self):
        self.receiver.join()
        self.sender.join()
        self.socket.close()


def receiver(skt, out_dir):
    files = {}
    wrote_header = {}
    ended = set()

    os.makedirs(out_dir, exist_ok=True)

    try:
        while True:
            batch = recv_batch(skt)
            qid = batch.get_query_id()

            if batch.is_last_batch():
                ended.add(qid)
                logging.info(f"[CLIENT] Recibido END de Q{qid}. Pendientes: {AMOUNT_OF_QUERIES - len(ended)}")
                if len(ended) >= AMOUNT_OF_QUERIES:
                    logging.info("[CLIENT] Recibidos END de todas las queries. Fin.")
                    break
                continue

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

    except Exception as e:
        logging.error(f"[CLIENT] Error en receiver: {e}")
    finally:
        for f in files.values():
            try:
                f.close()
            except Exception:
                pass
        logging.info(f"[CLIENT] Resultados guardados en {out_dir}")
