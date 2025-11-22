import logging
import signal
import sys
import os
from collections import defaultdict
from middleware.middleware import MessageMiddlewareQueue
from common.batch import Batch
from common.id_range_counter import IDRangeCounter
from configparser import ConfigParser
import socket
import threading

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])
HEALTH_PORT = 3030


class Accumulator:
    def __init__(self, consume_queue, produce_queue, *,
                 file_backup: str,
                 key_col: str,
                 value_col: str,
                 bucket_col: str,
                 out_value_name: str):
        self._consume_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=consume_queue)
        self._produce_queue = MessageMiddlewareQueue(host="rabbitmq", queue_name=produce_queue)

        self._key_col = key_col
        self._value_col = value_col
        self.bucket_col = bucket_col
        self._out_value_name = out_value_name

        self.client_state = defaultdict(lambda: {
            "accumulator": {},
            "expected": None,
            "received": 0,
            "type": None,
            "id_counter": IDRangeCounter(),
        })

        self._wal_dir = file_backup
        os.makedirs(self._wal_dir, exist_ok=True)
        try:
            if os.listdir(self._wal_dir):
                logging.info(f"[ACCUMULATOR] La carpeta de backups existe. creando estado de recovery...")
                self._load_wal()
        except FileNotFoundError:
            logging.info(f"[ACCUMULATOR] La carpeta de backups no existe.")
            os.makedirs(self._wal_dir, exist_ok=True)
        
        self._health_sock = None
        self._health_thread = None

        signal.signal(signal.SIGTERM, self.graceful_quit)

    def _wal_path(self, cid):
        """Devuelve la ruta del WAL para un client_id."""
        return os.path.join(self._wal_dir, f"acc_client_{cid}.wal")

    def _wal_append(self, cid, lines, batch_id):
        """
        Escribe una lista de líneas en el WAL del cliente (append seguro).
        lines: lista de strings SIN '\n'.
        """
        if not lines:
            return
        path = self._wal_path(cid)
        data = "".join(line + "\n" for line in lines)
        data = "BEGIN\n" + data +str(batch_id)+";END\n"
        b = data.encode("utf-8")

        flags = os.O_CREAT | os.O_WRONLY | os.O_APPEND
        try:
            fd = os.open(path, flags, 0o644)
            try:
                os.write(fd, b)
                os.fsync(fd)
            finally:
                os.close(fd)
        except Exception as e:
            logging.error(f"[ACCUMULATOR][WAL] Error escribiendo {path}: {e}")

    def _wal_remove(self, cid):
        """Elimina el WAL del cliente cuando ya no lo necesitamos."""
        path = self._wal_path(cid)
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logging.error(f"[ACCUMULATOR][WAL] Error eliminando {path}: {e}")


    def _replay_block(self, state, cid, block_lines, end_batch_id_str):
        if not block_lines:
            return

        if block_lines[0] == "LAST":
            if len(block_lines) < 2:
                logging.warning(f"[ACCUMULATOR][WAL] bloque LAST incompleto para cid={cid}: {block_lines!r}")
                return

            parts = block_lines[1].split(";")
            if len(parts) < 2:
                logging.warning(f"[ACCUMULATOR][WAL] linea LAST inválida para cid={cid}: {block_lines[1]!r}")
                return

            try:
                bid = int(parts[0])
                expected = int(parts[1])
            except ValueError:
                logging.warning(f"[ACCUMULATOR][WAL] valores LAST inválidos para cid={cid}: {block_lines[1]!r}")
                return

            state["expected"] = expected
            if not state["id_counter"].already_processed(bid, ' '):
                state["id_counter"].add_id(bid, ' ')
            return

        try:
            bid = int(end_batch_id_str)
        except ValueError:
            logging.warning(f"[ACCUMULATOR][WAL] batch_id de END inválido para cid={cid}: {end_batch_id_str!r}")
            return

        for line in block_lines:
            parts = line.split(";")
            if len(parts) != 3:
                logging.warning(f"[ACCUMULATOR][WAL] linea de datos inválida para cid={cid}: {line!r}")
                continue
            bucket, key, value_str = parts
            try:
                delta = float(value_str)
            except ValueError:
                logging.warning(f"[ACCUMULATOR][WAL] delta inválida para cid={cid}: {line!r}")
                continue

            acc_key = (bucket, key)
            state["accumulator"][acc_key] = state["accumulator"].get(acc_key, 0.0) + delta

        if not state["id_counter"].already_processed(bid, ' '):
            state["id_counter"].add_id(bid, ' ')
            state["received"] += 1

    def _load_wal(self):
        try:
            for fname in os.listdir(self._wal_dir):
                if not fname.startswith("acc_client_") or not fname.endswith(".wal"):
                    continue

                cid_str = fname[len("acc_client_"):-len(".wal")]
                cid = cid_str

                path = os.path.join(self._wal_dir, fname)
                state = self.client_state[cid]

                state["accumulator"].clear()
                state["expected"] = None
                state["received"] = 0
                state["type"] = None
                state["id_counter"] = IDRangeCounter()

                try:
                    with open(path, "r", encoding="utf-8") as f:
                        in_block = False
                        block_lines = []
                        end_batch_id = None

                        for raw in f:
                            line = raw.rstrip("\n")
                            if not line:
                                continue

                            if line == "BEGIN":
                                in_block = True
                                block_lines = []
                                end_batch_id = None
                                continue

                            if not in_block:
                                continue

                            if line.endswith(";END"):
                                end_batch_id = line.split(";", 1)[0]
                                self._replay_block(state, cid, block_lines, end_batch_id)
                                in_block = False
                                block_lines = []
                                end_batch_id = None
                                continue

                            block_lines.append(line)

                except Exception as e:
                    logging.error(f"[ACCUMULATOR][WAL] Error leyendo {path}: {e}")
                    continue

                logging.info(
                    f"[ACCUMULATOR][RECOVERY] client_id={cid} "
                    f"entries={len(state['accumulator'])} "
                    f"expected={state['expected']} received={state['received']}"
                )
        except FileNotFoundError:
            return
        except Exception as e:
            logging.error(f"[ACCUMULATOR][RECOVERY] Error recorriendo directorio WAL: {e}")

    def graceful_quit(self, signum, frame):
        logging.debug("Recibida señal SIGTERM, cerrando aggregator...")
        self.stop()
        logging.debug("Aggregator cerrado correctamente.")
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
        self._health_thread.start()
        
        self._consume_queue.start_consuming(self.callback)

    def stop(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        self._produce_queue.close()

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        cid = str(batch.client_id())
        state = self.client_state[cid]

        if state["id_counter"].already_processed(batch.id(), ' '):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if batch.is_last_batch():
            try:
                idx = batch.get_header().index('cant_batches')
                state["expected"] = int(batch[0][idx])
                logging.debug(f"[aggregator] cid={cid} espera {state['expected']} batches")
            except (ValueError, IndexError):
                logging.warning(f"[aggregator] cid={cid} last_batch sin 'cant_batches' válido")

            expected = state["expected"] if state["expected"] is not None else 0
            wal_line = f"LAST\n{batch.id()};{expected};"
            self._wal_append(cid, [wal_line], batch.id())

            state["id_counter"].add_id(batch.id(), ' ')

            if state["expected"] is not None and state["received"] == state["expected"]:
                logging.debug(f"[aggregator] flusheo pq state[received] == state[expected]")
                self._flush_client(cid)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        state["type"] = batch.type()
        wal_lines = []

        for row in batch.iter_per_header():
            try:
                bucket = row[self.bucket_col]
                key = row[self._key_col]
                value = float(row[self._value_col])
                acc_key = (bucket, key)
                state["accumulator"][acc_key] = state["accumulator"].get(acc_key, 0.0) + value
                wal_lines.append(f"{bucket};{key};{value}")
            except Exception:
                logging.error(f"[aggregator] cid={cid} row malformed: {row}")
                continue

        state["received"] += 1
        state["id_counter"].add_id(batch.id(), ' ')
        logging.debug(f"[aggregator] cid={cid} received={state['received']}")

        self._wal_append(cid, wal_lines, batch.id())

        if state["expected"] is not None and state["received"] == state["expected"]:
            logging.debug(f"[aggregator]llegue a la cantidad esperada")
            self._flush_client(cid)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _flush_client(self, cid: int):
        state = self.client_state[cid]
        if not state["accumulator"]:
            state["expected"] = None
            state["received"] = 0
            state["type"] = None
            state["id_counter"] = IDRangeCounter()
            self._wal_remove(cid)
            return

        header = [self.bucket_col, self._key_col, self._out_value_name]
        rows = [[bucket, str(key), f"{total:.2f}"] for (bucket, key), total in state["accumulator"].items()]

        total_rows = len(rows)
        count_batches = 0
        for i in range(0, total_rows, BUFFER_SIZE):
            chunk = rows[i:i + BUFFER_SIZE]
            out_batch = Batch(
                id=count_batches,
                query_id=0,
                client_id=cid,
                last=False,
                type_file=state["type"],
                header=header,
                rows=chunk
            )
            self._produce_queue.send(out_batch.encode())
            count_batches += 1

        last_batch = Batch(
            id=count_batches,
            query_id=0,
            client_id=int(cid),
            last=True,
            type_file=state["type"],
            header=['cant_batches'],
            rows=[[str(count_batches)]],
        )
        self._produce_queue.send(last_batch.encode())

        state["accumulator"].clear()  # TODO: borrar los states
        state["expected"] = None
        state["received"] = 0
        state["type"] = None
        state["id_counter"] = IDRangeCounter()

        self._wal_remove(cid)
