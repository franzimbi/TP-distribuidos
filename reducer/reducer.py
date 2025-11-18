from common.batch import Batch
import logging
import heapq
import signal
import sys
import os


class Reducer:
    def __init__(self, consumer, producer, top, columns, path_log_file):
        self._consumer_queue = consumer
        self._producer_queue = producer
        try:
            if  os.listdir(path_log_file):
                logging.info(f"[REDUCER] La carpeta de backups existe. creando estado de recovery...")
        except FileNotFoundError as e:
            logging.info(f"[REDUCER] La carpeta de backups no existe.")
            os.makedirs(path_log_file)
        nombre_asignado = ""
        try:
            with open("/etc/hostname", "r") as f:
                nombre_asignado = f.read().strip()
        except Exception as e:
            return f"Error al leer el hostname: {e}"
            
        self.log_file_path = path_log_file + f'/{nombre_asignado}.txt'
        self._top = int(top)
        self.top_users = {}  # key: store_id, value: dict of user_id -> purchases_qty
        self.counter_batches = {}
        self.waited_batches = {}
        self._columns = [n.strip() for n in columns.split(",")]  # store_id, user_id,purchases_qty

        logging.debug(f"[REDUCER] Initialized with top={self._top} and columns={self._columns}")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        try:
            logging.debug("[REDUCER] Recibida señal SIGTERM, cerrando reducer...")
            self.close()
            logging.debug("[REDUCER] Apagado limpio.")
        except Exception as e:
            logging.error(f"[REDUCER] Error closing: {e}")
        sys.exit(0)

    def start(self):
        self._consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        client_id = batch.client_id()

        if client_id not in self.counter_batches:
            self.counter_batches[client_id] = 0
            self.waited_batches[client_id] = None

        # Si es el batch de control (cant_batches)
        if batch.is_last_batch():
            self.waited_batches[client_id] = int(batch[0][batch.get_header().index('cant_batches')])
            if self.waited_batches[client_id] == self.counter_batches[client_id]:  # llegaron todos
                self.send_last_batch(batch, client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

        # Acá procesamos los datos "normales"
        lines_to_write = []  # una línea por store cuyo top haya cambiado

        for row in batch.iter_per_header():
            store = row[self._columns[0]]
            user = row[self._columns[1]]
            qty = int(float(row[self._columns[2]]))

            if client_id not in self.top_users:
                self.top_users[client_id] = {}
            if store not in self.top_users[client_id]:
                self.top_users[client_id][store] = {}

            # Top anterior de ESTE store (para este client_id)
            prev_top = self.top_users[client_id][store].copy()

            # Actualizo cantidad del usuario
            self.top_users[client_id][store][user] = qty

            # Recalculo top N
            store_dict = self.top_users[client_id][store]
            top_keys = heapq.nlargest(self._top, store_dict, key=store_dict.get)
            new_top = {k: store_dict[k] for k in top_keys}

            # Piso el dict del store con sólo el top N
            self.top_users[client_id][store] = new_top

            # Sólo si el top cambió respecto del anterior, genero línea para disco
            if new_top != prev_top:
                to_disk = (
                    f"top_users,{client_id},{store},"
                    + ",".join(f"{u}:{q}" for u, q in new_top.items())
                )
                lines_to_write.append(to_disk)

        # Terminé de procesar el batch
        self.counter_batches[client_id] += 1

        # Si ya llegaron todos los batches de este cliente, mando el último batch
        if (
            self.waited_batches[client_id] is not None
            and self.counter_batches[client_id] == self.waited_batches[client_id]
        ):
            self.send_last_batch(batch, client_id)

        # Persisto SOLO los cambios detectados
        for line in lines_to_write:
            logging.info(
                f"[REDUCER] guardo en disco: {line[0:10]}. en self.log_file_path: {self.log_file_path}"
            )
            self.register_to_disk(line, self.log_file_path)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def register_to_disk(self, register, log_file_path):
        # if register == '':
        #     return True
        try:
            logging.info(f"[LOG] Escribiendo en {log_file_path}:")
            with open(log_file_path, "a") as f:
                f.write(register + "\nCHECKPOINT!!COPADO\n")
                logging.info(f"[LOG] ok. flush")
                f.flush()
                logging.info(f"[LOG] ok")
                logging.info(f"[LOG] fsync...")
                os.fsync(f.fileno()) 
                logging.info(f"[LOG] ok. cerrado.")
            return True
        except Exception as e:
            logging.error(f"[LOG] Error escribiendo entradas atomicas a {log_file_path}: {e}")
            return False

    def send_last_batch(self, batch, client_id):
        rows = []
        try:
            for store, users in self.top_users[client_id].items():
                for user, qty in users.items():
                    rows.append((store, user, qty))
            rows = [(str(int(float(s))), str(int(float(u))), str(float(v))) for (s, u, v) in rows]
            rows.sort(key=lambda x: (int(x[0]), int(x[1])))
        except Exception as e:
            logging.error(f"[REDUCER] Error sending last batch: {e}")

        batch_result = Batch(batch.id() - 1, client_id=client_id, type_file=batch.type())
        batch_result.set_header([self._columns[0], self._columns[1], self._columns[2]])
        for store, user, qty in rows:
            batch_result.add_row([store, user, str(qty)])
        batch_result.set_client_id(client_id)
        self._producer_queue.send(batch_result.encode())

        batch.delete_rows()
        batch.set_last_batch(True)
        batch.set_header(['cant_batches'])
        batch.add_row([str(1)])
        self._producer_queue.send(batch.encode())
        self.top_users.pop(client_id)
        self.waited_batches.pop(client_id)
        self.counter_batches.pop(client_id)

    def close(self):
        self._consumer_queue.stop_consuming()
        self._consumer_queue.close()
        self._producer_queue.close()
        logging.debug("[REDUCER] Queues cerradas")
