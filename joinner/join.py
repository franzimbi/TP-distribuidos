from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareExchange
from common.batch import Batch
from common.id_range_counter import IDRangeCounter
import logging
import signal
import sys
import threading
import socket
import os
import json
import tempfile

HEALTH_PORT = 3030

class Join:
    def __init__(self, confirmation_queue, join_queue, column_id, column_name, is_last_join, backup_dir):
        self.producer_queue = None
        self.consumer_queue = None
        self.confirmation_queue = confirmation_queue
        self.join_dictionary = {}
        self.is_last_join = is_last_join
        self.batch_counter = 0
        self.counter_batches = {}
        self.waited_batches = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = join_queue
        self.thead_join = None
        self.lock = threading.Lock()

        self.backup_dir = backup_dir
        os.makedirs(self.backup_dir, exist_ok=True)

        self.snapshot_path = os.path.join(self.backup_dir, "join_snapshot.json")
        self.load_snapshot()

        self._health_thread = None
        self._health_sock = None
        signal.signal(signal.SIGTERM, self.graceful_quit)
        signal.signal(signal.SIGINT, self.graceful_quit)

    def graceful_quit(self, signum, frame):
        try:
            logging.debug("Recibida señal SIGTERM, cerrando joiner...")
            self.close()
            logging.debug("Joiner cerrado correctamente.")
        except Exception as e:
            logging.error(f"[JOIN] Error en graceful_quit: {e}")
        sys.exit(0)

    def _backup_path_for_client(self, client_id):
        client_id = str(client_id)
        return os.path.join(self.backup_dir, f"client_{client_id}_backup.csv")

    def _safe_append(self, path, content):
        b = content.encode('utf-8')
        flags = os.O_CREAT | os.O_WRONLY | os.O_APPEND
        try:
            fd = os.open(path, flags, 0o644)
            try:
                os.write(fd, b)
                os.fsync(fd)
            finally:
                os.close(fd)
            return True
        except Exception as e:
            logging.error(f"[JOIN] Error escribiendo append seguro a {path}: {e}")
            try:
                if 'fd' in locals():
                    os.close(fd)
            except Exception:
                pass
            return False

    def _remove_backup_file(self, client_id):
        path = self._backup_path_for_client(client_id)
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logging.error(f"[JOIN] Error eliminando backup {path}: {e}")

    def _write_to_backup(self, client_id, dictionary):
        if not dictionary:
            return True

        path = self._backup_path_for_client(client_id)
        lines = []
        for k, v in dictionary.items():
            lines.append(f"{k},{v}")
        content = "\n".join(lines) + "\n"
        ok = self._safe_append(path, content)
        if not ok:
            logging.error(f"[JOIN] fallo backup append para client {client_id} de {len(dictionary)} entradas")
        return ok

    def _load_backups_on_start(self):
        print("\n\nentre a LOad_backups_on_start\n")
        print(f"recorro el {self.backup_dir} que tiene {os.listdir(self.backup_dir)}")

        for fname in os.listdir(self.backup_dir):
            if not fname.startswith("client_") or not fname.endswith("_backup.csv"):
                continue
            client_id = fname[len("client_"):-len("_backup.csv")]
            path = os.path.join(self.backup_dir, fname)
            d = {}
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for raw in f:
                        if not raw.endswith("\n"):
                            logging.warning(f"[JOIN] linea incompleta por crash: {raw!r}")
                            continue
                        line = raw.rstrip("\n")
                        if not line:
                            continue
                        if "," not in line:
                            logging.warning(f"[JOIN] linea corrupta en {path}: {line!r}, ignoro")
                            continue
                        key, value = line.split(",", 1)
                        key = str(key).strip()
                        value = str(value).strip()
                        if key not in d:
                            d[key] = value
            except Exception as e:
                logging.error(f"[JOIN] Error leyendo backup {path}: {e}")
                continue
            if d:
                self.join_dictionary[client_id] = d
                self.load_snapshot()
                print(f"levante del archivo {path} el diccionario")
                logging.info(f"[JOIN] Restaurado backup para client_id={client_id} con {len(d)} entradas")
        print("salgo del load_backup\n\n")

    def register_to_disk(self, content, path):
        dirpath = os.path.dirname(path)
        os.makedirs(dirpath, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dirpath)
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
            return True
        except Exception as e:
            logging.error(f"[JOIN] Error escribiendo snapshot atómico {path}: {e}")
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return False
        
    def write_snapshot(self, client_id):
        snapshot = {
            "client_id": client_id,
            "counter_batches": self.counter_batches[client_id].to_dict(),
            "waited_batches": self.waited_batches.get(client_id),
        }
        
        json_str = json.dumps(snapshot, indent=2)
        self.register_to_disk(json_str, self.snapshot_path)

    def load_snapshot(self):
        if not os.path.exists(self.snapshot_path):
            return
        try:
            with open(self.snapshot_path, "r", encoding="utf-8") as f:
                snapshot = json.load(f)
            client_id  = snapshot["client_id"]
            self.waited_batches[client_id] = snapshot.get("waited_batches")
            counter = IDRangeCounter()
            counter = IDRangeCounter.from_dict(snapshot.get("counter_batches", {}))
            self.counter_batches[client_id] = counter

            logging.info(f"[JOIN][RECOVERY] Snapshot cargado para client_id={client_id}")
        except Exception as e:
            logging.error(f"[JOIN][RECOVERY] Error cargando snapshot: {e}")
            
    def start(self, consumer, producer):
        self._health_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._health_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._health_sock.bind(('', HEALTH_PORT))
        self._health_sock.listen()

        def loop():
            while True:
                conn, addr = self._health_sock.accept()
                conn.close()

        self._health_thread = threading.Thread(target=loop, daemon=True)
        self._health_thread.start()

        self._load_backups_on_start()
        aux = self.join_queue
        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=aux)
        queue_confirm_name = self.confirmation_queue
        self.confirmation_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_confirm_name)

        self.thead_join = threading.Thread(target=self.join_queue.start_consuming,
                                           args=(self.callback_to_receive_join_data,), daemon=True)
        self.thead_join.start()

        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)

        self.consumer_queue.start_consuming(self.callback)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        self.batch_counter += 1
        batch = Batch()
        batch.decode(message)
        client_id = str(batch.client_id()).strip()

        batch_changes = {}

        if client_id not in self.counter_batches:
            self.counter_batches[client_id] = IDRangeCounter()
            self.waited_batches[client_id] = None

        if self.counter_batches[client_id].already_processed(batch.id(), ' '):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        id_idx = batch.index_of(self.column_id)
        name_idx = batch.index_of(self.column_name)

        if batch.is_last_batch():
            self.confirmation_queue.send(batch.encode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if id_idx is None or name_idx is None:
            logging.error(
                f"Column {self.column_id} or {self.column_name} not found in batch header {batch.get_header()}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        for row in batch:
            key = str(row[id_idx]).strip()
            value = str(row[name_idx]).strip()
            if key is None or value is None:
                logging.error(f"Key or value is None for row: {row}")
                continue
            with self.lock:
                if client_id not in self.join_dictionary:
                    self.join_dictionary[client_id] = {}
                if key not in self.join_dictionary[client_id]:
                    self.join_dictionary[client_id][key] = value
                    batch_changes[key] = value
        if batch_changes:
            self._write_to_backup(client_id, batch_changes)
            self.write_snapshot(client_id)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = str(batch.client_id()).strip()

        if batch.is_last_batch():
            self.waited_batches[client_id] = int(batch[0][batch.get_header().index('cant_batches')])
            if self.waited_batches[client_id] == self.counter_batches[client_id].amount_ids(' '):  # llegaron todos
                self.join_dictionary.pop(client_id, None)
                self._remove_backup_file(client_id)
            self.producer_queue.send(batch.encode())

            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            with self.lock:
                batch.change_header_name_value(
                    self.column_id,
                    self.column_name,
                    self.join_dictionary[client_id],
                    self.is_last_join
                )
        except (ValueError, KeyError) as e:
            logging.error(
                f'action: join_batch_with_dicctionary[{client_id}] | result: fail | error: {e}')

        self.counter_batches[client_id].add_id(batch.id(), ' ')
        if self.waited_batches[client_id] is not None and self.counter_batches[client_id] == self.waited_batches[
            client_id]:
            self.join_dictionary.pop(client_id, None)
            self._remove_backup_file(client_id)

        self.write_snapshot(client_id)

        self.producer_queue.send(batch.encode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def close(self):
        try:
            if self.consumer_queue:
                self.consumer_queue.stop_consuming()
                self.consumer_queue.close()

            if self.producer_queue:
                self.producer_queue.close()

            if self.join_queue:
                self.join_queue.stop_consuming()
                self.join_queue.close()

            if self._health_sock:
                self._health_sock.close()

            if self.thead_join is not None:
                self.thead_join.join()
        except Exception:
            pass
