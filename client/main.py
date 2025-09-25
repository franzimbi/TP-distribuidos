#!/usr/bin/env python3
import os
import socket
import threading
from common.protocol import send_batches_from_csv, recv_batches_from_socket

HOST = os.getenv("DISTRIBUTOR_HOST")
PORT = int(os.getenv("DISTRIBUTOR_PORT"))

# Abrimos el archivo de salida
os.makedirs('results', exist_ok=True)
fileQ1 = open('results/q1.csv', 'w', buffering=1)  # line-buffered para que flushee seguido

counter = 0
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

# ----- Sender -----
sender = threading.Thread(
    target=send_batches_from_csv,
    args=('csvs_files/transactions', 150, sock, 't'),
    daemon=True,
)
sender.start()

# ----- Receiver -----
def receiver():
    global counter
    try:
        for batch in recv_batches_from_socket(sock):
            if batch.is_last_batch():
                print("[CLIENT] Recibido END, fin de procesamiento.")
                break

            # Log correcto del ID
            try:
                print(f"[CLIENT] Llegó batch id={batch.id()} con {len(batch)} filas")
            except Exception:
                print("[CLIENT] Llegó batch (no se pudo imprimir id)")

            # Escribimos filas al CSV de salida
            for row in batch:
                # row es lista de columnas; volvemos a CSV (separador coma)
                fileQ1.write(",".join(row) + "\n")
                counter += 1

        print(f"[CLIENT] Cerrando receiver. Total filas escritas: {counter}")
    except Exception as e:
        print(f"[CLIENT] Error en receiver: {e}")

receiver_t = threading.Thread(target=receiver, daemon=True)
receiver_t.start()

# ----- Espera ordenada -----
receiver_t.join()
sender.join()

try:
    sock.shutdown(socket.SHUT_RDWR)
except Exception:
    pass
sock.close()
fileQ1.close()

print(f"\nQ1: {counter} rows received\n")
