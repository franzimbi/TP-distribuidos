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
import time

config = ConfigParser()
config.read("config.ini")

BUFFER_SIZE = int(config["DEFAULT"]["BATCH_SIZE"])
HEALTH_PORT = 3030

CHECKPOINT_INTERVAL = 2500

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

        self.has_last_batch = False
        self.printed_last = False

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

        self.client_state = defaultdict(lambda: {
            "accumulator": {},
            "expected": None,
            "received": 0,
            "type": None,
            "id_counter": IDRangeCounter(),
            "last_checkpoint": 0,
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

        # self._health_sock = None
        # self._health_thread = None

        signal.signal(signal.SIGTERM, self.graceful_quit)

    def _wal_path(self, cid):
        """Devuelve la ruta del WAL para un client_id."""
        return os.path.join(self._wal_dir, f"acc_client_{cid}.wal")
    
    def _snapshot_path(self, cid):
        """Devuelve la ruta del snapshot compactado para un client_id."""
        return os.path.join(self._wal_dir, f"acc_client_{cid}.snapshot")

    def _wal_append(self, cid, lines, batch_id):
        """
        Escribe una lista de líneas en el WAL del cliente (append seguro).
        lines: lista de strings SIN '\n'.
        """
        if not lines:
            return
        path = self._wal_path(cid)
        data = "".join(line + "\n" for line in lines)
        data = "BEGIN\n" + data + str(batch_id) + ";END\n"
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
        snap_path = self._snapshot_path(cid)
        try:
            if os.path.exists(path):
                os.remove(path)
            if os.path.exists(snap_path):
                os.remove(snap_path)
        except Exception as e:
            logging.error(f"[ACCUMULATOR][WAL] Error eliminando {path}: {e}")
    
    def _compact_wal(self, cid):
        """
        Compacta el WAL escribiendo un snapshot atómico del accumulator actual
        y limpiando el WAL incremental. Esto evita que el WAL crezca sin límite.
        """
        state = self.client_state[cid]
        snap_path = self._snapshot_path(cid)
        
        try:
            import tempfile
            dir_path = os.path.dirname(snap_path)
            tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
            
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(f"SNAPSHOT\n")
                f.write(f"expected={state['expected']}\n")
                f.write(f"received={state['received']}\n")
                f.write(f"type={state['type']}\n")
                f.write(f"DATA\n")
                
                for (bucket, key), value in state["accumulator"].items():
                    f.write(f"{bucket};{key};{value}\n")
                
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(tmp_path, snap_path)
            
            wal_path = self._wal_path(cid)
            if os.path.exists(wal_path):
                os.remove(wal_path)
            
            state["last_checkpoint"] = state["received"]
            
        except Exception as e:
            logging.error(f"[ACCUMULATOR][COMPACT] Error compactando cid={cid}: {e}")
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except:
                pass

    def _replay_block(self, state, cid, block_lines, end_batch_id_str):
        if not block_lines:
            return

        if block_lines[0] == "LAST":
            if len(block_lines) < 2:
                logging.info(f"[ACCUMULATOR][WAL] bloque LAST incompleto para cid={cid}: {block_lines!r}")
                return

            parts = block_lines[1].split(";")
            if len(parts) < 2:
                logging.info(f"[ACCUMULATOR][WAL] linea LAST inválida para cid={cid}: {block_lines[1]!r}")
                return

            try:
                bid = int(parts[0])
                expected = int(parts[1])
            except ValueError:
                logging.info(f"[ACCUMULATOR][WAL] valores LAST inválidos para cid={cid}: {block_lines[1]!r}")
                return

            state["expected"] = expected
            if not state["id_counter"].already_processed(bid, ' ') and block_lines[0] != "LAST":
                state["id_counter"].add_id(bid, ' ')
            return

        try:
            bid = int(end_batch_id_str)
        except ValueError:
            logging.info(f"[ACCUMULATOR][WAL] batch_id de END inválido para cid={cid}: {end_batch_id_str!r}")
            return

        for line in block_lines:
            parts = line.split(";")
            if len(parts) != 3:
                logging.info(f"[ACCUMULATOR][WAL] linea de datos inválida para cid={cid}: {line!r}")
                continue
            bucket, key, value_str = parts
            try:
                delta = float(value_str)
            except ValueError:
                logging.info(f"[ACCUMULATOR][WAL] delta inválida para cid={cid}: {line!r}")
                continue

            acc_key = (bucket, key)
            state["accumulator"][acc_key] = state["accumulator"].get(acc_key, 0.0) + delta

        if not state["id_counter"].already_processed(bid, ' '):
            state["id_counter"].add_id(bid, ' ')
            state["received"] += 1

    def _load_wal(self):
        """
        Carga el estado persistido desde disco.
        Estrategia:
        1. Cargar snapshots (si existen)
        2. Aplicar WAL incremental sobre el snapshot (si existe)
        """
        try:
            snapshot_clients = set()
            for fname in os.listdir(self._wal_dir):
                if fname.startswith("acc_client_") and fname.endswith(".snapshot"):
                    cid_str = fname[len("acc_client_"):-len(".snapshot")]
                    cid = cid_str
                    snap_path = os.path.join(self._wal_dir, fname)
                    self._load_snapshot(cid, snap_path)
                    snapshot_clients.add(cid)
            
            for fname in os.listdir(self._wal_dir):
                if fname.startswith("acc_client_") and fname.endswith(".wal"):
                    cid_str = fname[len("acc_client_"):-len(".wal")]
                    cid = cid_str
                    path = os.path.join(self._wal_dir, fname)
                    
                    if cid in snapshot_clients:
                        self._apply_incremental_wal(cid, path)
                    else:
                        self._load_wal_file(cid, path)

        except FileNotFoundError:
            return
        except Exception as e:
            logging.info(f"[ACCUMULATOR][RECOVERY] Error recorriendo directorio WAL: {e}")
    
    def _apply_incremental_wal(self, cid, wal_path):
        """
        Aplica un WAL incremental sobre un estado ya cargado desde snapshot.
        Esto permite recuperar batches procesados después del último checkpoint.
        """
        state = self.client_state[cid]
        
        try:
            with open(wal_path, "r", encoding="utf-8") as f:
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

            # logging.info(
            #     f"[ACCUMULATOR][RECOVERY] incremental wal cid={cid} "
            #     f"entries={len(state['accumulator'])} received={state['received']}"
            # )
        except Exception as e:
            logging.error(f"[ACCUMULATOR][WAL] Error aplicando WAL incremental {wal_path}: {e}")
    
    def _load_snapshot(self, cid, snap_path):
        """Carga un snapshot compactado."""
        state = self.client_state[cid]
        state["accumulator"].clear()
        state["expected"] = None
        state["received"] = 0
        state["type"] = None
        state["id_counter"] = IDRangeCounter()
        
        try:
            with open(snap_path, "r", encoding="utf-8") as f:
                mode = None
                for raw in f:
                    line = raw.rstrip("\n")
                    if not line:
                        continue
                    
                    if line == "SNAPSHOT":
                        continue
                    if line == "DATA":
                        mode = "data"
                        continue
                    
                    if mode != "data":
                        if line.startswith("expected="):
                            val = line.split("=", 1)[1]
                            state["expected"] = int(val) if val != "None" else None
                        elif line.startswith("received="):
                            state["received"] = int(line.split("=", 1)[1])
                        elif line.startswith("type="):
                            val = line.split("=", 1)[1]
                            state["type"] = val if val != "None" else None
                    else:
                        # Parsear datos
                        parts = line.split(";")
                        if len(parts) != 3:
                            continue
                        bucket, key, value_str = parts
                        try:
                            value = float(value_str)
                            state["accumulator"][(bucket, key)] = value
                        except ValueError:
                            continue
            
            logging.info(
                f"[ACCUMULATOR][RECOVERY] snapshot cid={cid} "
                f"entries={len(state['accumulator'])} "
                f"expected={state['expected']} received={state['received']}"
            )
        except Exception as e:
            logging.error(f"[ACCUMULATOR][RECOVERY] Error leyendo snapshot {snap_path}: {e}")
    
    def _load_wal_file(self, cid, path):
        """Carga un archivo WAL incremental (legacy)."""
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
            return

        logging.info(
            f"[ACCUMULATOR][RECOVERY] wal cid={cid} "
            f"entries={len(state['accumulator'])} "
            f"expected={state['expected']} received={state['received']}"
        )

    def graceful_quit(self, signum, frame):
        logging.info("Recibida señal SIGTERM, cerrando aggregator...")
        self.stop()
        logging.info("Aggregator cerrado correctamente.")
        sys.exit(0)

    def start(self):
        self._consume_queue.start_consuming(self.callback)

    def stop(self):
        self._consume_queue.stop_consuming()
        self._consume_queue.close()
        self._produce_queue.close()

    def callback(self, ch, method, properties, message):
        try:
            batch = Batch()
            batch.decode(message)
            cid = str(batch.client_id())
            state = self.client_state[cid]

            if "last_checkpoint" not in state:
                state["last_checkpoint"] = 0

            if state["id_counter"].already_processed(batch.id(), ' '):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if batch.is_last_batch():
                try:
                    self.has_last_batch = True
                    idx = batch.get_header().index('cant_batches')
                    state["expected"] = int(batch[0][idx])
                    # state["id_counter"].add_id(batch.id(), ' ')
                    # state["received"] += 1
                    logging.info(f"[aggregator] cid={cid} espera {state['expected']} batches")
                except (ValueError, IndexError):
                    logging.info(f"[aggregator] cid={cid} last_batch sin 'cant_batches' válido")

                expected = state["expected"] if state["expected"] is not None else 0
                wal_line = f"LAST\n{batch.id()};{expected};"
                self._wal_append(cid, [wal_line], batch.id())

                # if state["expected"] is not None and state["received"] == state["expected"]:
                if state["expected"] is not None and state["id_counter"].amount_ids(' ') == state["expected"]:
                    logging.info(f"[aggregator] flusheo pq state[received] == state[expected]")
                    self._flush_client(cid)
                else:
                    logging.info(f"[aggregator] no flusheo pq state[received] != state[expected]")
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
                    logging.info(f"[aggregator] cid={cid} row malformed: {row}")
                    continue

            state["received"] += 1
            state["id_counter"].add_id(batch.id(), ' ')
            
            if self.has_last_batch:
                logging.info(f"[aggregator] cid={cid}, batch.id()={batch.id()}, received={state['received']}, expected={state['expected']}")

            self._wal_append(cid, wal_lines, batch.id())

            batches_since_checkpoint = state["received"] - state["last_checkpoint"]
            
            if batches_since_checkpoint >= CHECKPOINT_INTERVAL:
                self._compact_wal(cid)

            # if state["expected"] is not None and state["received"] == state["expected"]:
            if state["expected"] is not None and state["id_counter"].amount_ids(' ') == state["expected"]:
                logging.info(f"[aggregator]llegue a la cantidad esperada")
                self._flush_client(cid)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            if self.has_last_batch and self.printed_last is False:
                self.printed_last = True
                print("ya ackee el last batch, tirame bro \n")
                time.sleep(10)
        except Exception as e:
            logging.info(f"[ACCUMULATOR] Error en callback principal: {e}")

    def _flush_client(self, cid: int):
        state = self.client_state[cid]
        if not state["accumulator"]:
            state["expected"] = None
            state["received"] = 0
            state["type"] = None
            state["id_counter"] = IDRangeCounter()
            state["last_checkpoint"] = 0
            # self._wal_remove(cid)
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

        state["accumulator"].clear()
        state["expected"] = None
        state["received"] = 0
        state["type"] = None
        state["id_counter"] = IDRangeCounter()
        state["last_checkpoint"] = 0

        #self._wal_remove(cid)
