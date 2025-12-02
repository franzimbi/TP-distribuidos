from common.batch import Batch
from common.id_range_counter import IDRangeCounter
import logging
import heapq
import signal
import sys
import os
import json
import tempfile
import socket
import threading

HEALTH_PORT = 3030


class Reducer:
    def __init__(self, consumer, producer, top, columns, path_log_file):
        self._consumer_queue = consumer
        self._producer_queue = producer

        self._top = int(top)
        self.top_users = {}  # key: store_id, value: dict of user_id -> purchases_qty
        self.counter_batches = {}
        self.waited_batches = {}
        self._columns = [n.strip() for n in columns.split(",")]  # store_id, user_id,purchases_qty

        self.log_file_path = path_log_file + f'/snapshot.json'
        try:
            if os.listdir(path_log_file):
                logging.info(f"[REDUCER] La carpeta de backups existe. creando estado de recovery...")
                self.load_snapshot()
        except FileNotFoundError as e:
            logging.info(f"[REDUCER] La carpeta de backups no existe.")
            os.makedirs(path_log_file)

        self._health_sock = None
        self._health_thread = None

        logging.debug(f"[REDUCER] Initialized with top={self._top} and columns={self._columns}")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def load_snapshot(self):
        if not os.path.exists(self.log_file_path):
            return

        try:
            with open(self.log_file_path, "r") as f:
                snapshot = json.load(f)

            clients = snapshot.get("clients", {})

            for client_id, cdata in clients.items():
                self.top_users[client_id] = cdata.get("top_users", {})

                counter = IDRangeCounter.from_dict(cdata.get("counter_batches", {}))
                self.counter_batches[client_id] = counter

                self.waited_batches[client_id] = cdata.get("waited_batches")

            logging.info(f"[RECOVERY] Snapshot cargado: {len(clients)} clientes")

        except Exception as e:
            logging.error(f"[RECOVERY] Error cargando snapshot: {e}")

    def graceful_shutdown(self, signum, frame):
        try:
            logging.debug("[REDUCER] Recibida señal SIGTERM, cerrando reducer...")
            self.close()
            logging.debug("[REDUCER] Apagado limpio.")
        except Exception as e:
            logging.error(f"[REDUCER] Error closing: {e}")
        sys.exit(0)

    def start(self):
        self._health_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._health_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._health_sock.bind(('', HEALTH_PORT))
        self._health_sock.listen()

        def loop():
            while True:
                conn, addr = self._health_sock.accept()
                conn.close()

        self._health_thread = threading.Thread(target=loop, daemon=True)
        self._health_thread.start()  # no se esta joineando esto en graceful shutdown
        logging.info(f"[FILTER] Healthcheck escuchando en puerto {HEALTH_PORT}")

        self._consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):

        batch = Batch()
        batch.decode(message)
        client_id = str(batch.client_id())

        if client_id not in self.counter_batches:
            self.counter_batches[client_id] = IDRangeCounter()
            self.waited_batches[client_id] = None

        print(
            f"\n[REDUCER] llego {batch.id()} de client {client_id} y tengo counter:{self.counter_batches[client_id]} con waited: {self.waited_batches[client_id]}\n\n")

        if self.counter_batches[client_id].already_processed(batch.id(), ' '):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if batch.is_last_batch():
            self.waited_batches[client_id] = int(batch[0][batch.get_header().index('cant_batches')])
            if self.waited_batches[client_id] == self.counter_batches[client_id].amount_ids(' '):
                self.send_last_batch(batch, client_id)
                self.write_snapshot()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

        lines_to_write = []

        for row in batch.iter_per_header():
            store = row[self._columns[0]]
            user = row[self._columns[1]]
            qty = int(float(row[self._columns[2]]))

            if client_id not in self.top_users:
                self.top_users[client_id] = {}
            if store not in self.top_users[client_id]:
                self.top_users[client_id][store] = {}

            self.top_users[client_id][store][user] = qty

            store_dict = self.top_users[client_id][store]
            top_keys = heapq.nlargest(self._top, store_dict, key=store_dict.get)
            new_top = {k: store_dict[k] for k in top_keys}

            self.top_users[client_id][store] = new_top

        # termine de procesar el batch
        self.counter_batches[client_id].add_id(batch.id(), ' ')

        if (
                self.waited_batches[client_id] is not None
                and self.counter_batches[client_id].amount_ids(' ') + 1 == self.waited_batches[client_id]
        ):
            self.send_last_batch(batch, client_id)
        
        self.write_snapshot()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def write_snapshot(self):
        snapshot = {"clients": {}}

        for client_id in self.top_users.keys():
            snapshot["clients"][client_id] = {
                "top_users": self.top_users.get(client_id, {}),
                "counter_batches": self.counter_batches[client_id].to_dict(),
                "waited_batches": self.waited_batches.get(client_id)
            }

        json_str = json.dumps(snapshot, indent=2)
        self.register_to_disk(json_str, self.log_file_path)

    def register_to_disk(self, register, log_file_path):
        dirpath = os.path.dirname(log_file_path)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dirpath)

        try:
            with os.fdopen(tmp_fd, "w") as f:
                f.write(register)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, log_file_path)

            return True
        except Exception as e:
            logging.info(f"[LOG] Error escribiendo atomico {log_file_path}: {e}")
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return False

    def send_last_batch(self, batch, client_id):
        print(f"\n\n[REDUCER] paso 1\n\n\n")

        rows = []
        try:
            for store, users in self.top_users[client_id].items():
                for user, qty in users.items():
                    rows.append((store, user, qty))
            rows = [(str(int(float(s))), str(int(float(u))), str(float(v))) for (s, u, v) in rows]
            rows.sort(key=lambda x: (int(x[0]), int(x[1])))
        except Exception as e:
            logging.info(f"[REDUCER] Error sending last batch: {e}")

        client_id_int = int(client_id)

        print(f"\n\n[REDUCER] paso 2\n\n\n")
        batch_result = Batch(batch.id() - 1, client_id=client_id_int, type_file=batch.type())
        batch_result.set_header([self._columns[0], self._columns[1], self._columns[2]])
        for store, user, qty in rows:
            batch_result.add_row([store, user, str(qty)])
        batch_result.set_client_id(client_id_int)
        print(f"\n\n[REDUCER] paso 3\n\n\n")
        self._producer_queue.send(batch_result.encode())
        print(f"\n\n[REDUCER] paso 4\n\n\n")
        batch.delete_rows()
        batch.set_last_batch(True)
        batch.set_header(['cant_batches'])
        batch.add_row([str(1)])
        print(f"\n\n[REDUCER] paso 5\n\n\n")
        self._producer_queue.send(batch.encode())
        print(f"\n\n[REDUCER] paso 6 (final) \n\n\n")
        # self.top_users.pop(client_id)
        # self.waited_batches.pop(client_id)
        # self.counter_batches.pop(client_id)

    def close(self):
        self._consumer_queue.stop_consuming()
        self._consumer_queue.close()
        self._producer_queue.close()
        logging.debug("[REDUCER] Queues cerradas")
