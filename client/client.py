import socket
import threading
from common.protocol import send_batches_from_csv, recv_batches_from_socket

BATCH_SIZE = 150


class Client:

    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))

    def start(self, path_input, path_output):
        self.sender = threading.Thread(
            target=send_batches_from_csv,
            args=(path_input, BATCH_SIZE, self.socket, 't'),  # TODO sacar este 't'
            daemon=True
        )
        self.sender.start()
        self.receiver = threading.Thread(target=receiver, args=(self.socket, path_output), daemon=True)
        self.receiver.start()

    def close(self):
        self.receiver.join()
        self.sender.join()

        # try:
        #     self.socket.shutdown(socket.SHUT_RDWR)
        # except Exception:
        #     pass
        self.socket.close()


def receiver(skt, path):
    with open(path, 'w') as file:
        try:
            for batch in recv_batches_from_socket(skt):
                if batch.is_last_batch():
                    print("[CLIENT] Recibido END, fin de procesamiento.")
                    break
                print(f"[CLIENT] Llegó batch id={batch.id()} con {len(batch)} filas")

                for row in batch:
                    # row es lista de columnas; volvemos a CSV (separador coma)
                    file.write(",".join(row) + "\n")
        except Exception as e:
            print(f"[CLIENT] Error en receiver: {e}")
