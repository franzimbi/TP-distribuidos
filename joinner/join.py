from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareExchange
from common.batch import Batch
import logging
import signal
import sys
import threading
import socket
import os

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

        #/backups/joiner_Join_productos_Q21_backup

        # print(f"[joiner] Backup directory set to: {backup_dir}")
        # self.backup_dir = backup_dir
        # os.makedirs(self.backup_dir, exist_ok=True)

        # print(f"[joiner] Backup directory created if it did not exist: {self.backup_dir}")

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

    # def _backup_path_for_client(self, client_id):
    #     client_id = str(client_id)
    #     return os.path.join(self.backup_dir, f"join_{client_id}.csv")

    # def _append_backup_row(self, client_id, key, value):
    #     """Append de una fila id,nombre al CSV del client_id."""
    #     path = self._backup_path_for_client(client_id)
    #     try:
    #         with open(path, "a") as f:
    #             f.write(f"{key},{value}\n")
    #             f.flush()
    #             os.fsync(f.fileno())
    #     except Exception as e:
    #         logging.error(f"[JOIN] Error escribiendo backup de {client_id} en {path}: {e}")

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
        logging.info(f"[JOIN] escuchando a Healthcheck en puerto {HEALTH_PORT}")

        aux = self.join_queue
        self.join_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=aux)
        queue_confirm_name = self.confirmation_queue
        self.confirmation_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_confirm_name)

        self.thead_join = threading.Thread(target=self.join_queue.start_consuming,
                                    args=(self.callback_to_receive_join_data,), daemon=True)
        self.thead_join.start()
        
        logging.debug("action: receive_join_data | status: finished | entries: %d",
                      len(self.join_dictionary))

        self.consumer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consumer)
        self.producer_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer)

        self.consumer_queue.start_consuming(self.callback)

    def callback_to_receive_join_data(self, ch, method, properties, message):
        self.batch_counter += 1
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()

        if client_id not in self.counter_batches:
            self.counter_batches[client_id] = 0
            self.waited_batches[client_id] = None

        id_idx = batch.index_of(self.column_id)
        name_idx = batch.index_of(self.column_name)

        if batch.is_last_batch():
            self.confirmation_queue.send(batch.encode())
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if id_idx is None or name_idx is None:
            logging.debug(
                f"Column {self.column_id} or {self.column_name} not found in batch header {batch.get_header()}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        for row in batch:
            key = row[id_idx]
            value = row[name_idx]
            if key is None or value is None:
                logging.debug(f"Key or value is None for row: {row}")
                continue
            with self.lock:
                if client_id not in self.join_dictionary:
                    self.join_dictionary[client_id] = {}
                if key not in self.join_dictionary[client_id]:
                    self.join_dictionary[client_id][key] = value
                    # self._append_backup_row(client_id, key, value)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()

        if batch.is_last_batch():
            self.waited_batches[client_id] = int(batch[0][batch.get_header().index('cant_batches')])
            if self.waited_batches[client_id] == self.counter_batches[client_id]:  # llegaron todos
                self.join_dictionary.pop(client_id, None)
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

        self.counter_batches[client_id] += 1
        if self.waited_batches[client_id] is not None and self.counter_batches[client_id] == self.waited_batches[client_id]:
            self.join_dictionary.pop(client_id, None)

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
