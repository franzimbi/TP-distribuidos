#!/usr/bin/env python3
import os
import time
import socket
import threading
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import send_batches_from_csv
from common.protocol import recv_batches_from_socket
from common.protocol import Batch


host = os.getenv("DISTRIBUTOR_HOST")
port = int(os.getenv("DISTRIBUTOR_PORT"))
fileQ1 = open('results/q1.csv', 'w')
if fileQ1.closed:
    print("No se pudo abrir el archivo q1.csv")
    exit(1)
counter = 0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))

Q1sender_thread = threading.Thread(
    target=send_batches_from_csv,
    args=('csvs_files/transactions', 150, s, 't')
)
Q1sender_thread.start()


def receiver_thread_func():
    global counter
    for batch in recv_batches_from_socket(s):
        if batch.is_last_batch():
            print("Recibido END, fin de procesamiento.")
            break
        print(f"Llegó batch numero: {batch._id}\n")
        for line in ["|".join(row) for row in batch._body]:
            counter += 1
            fileQ1.write(line + '\n')

Q1receiver_thread = threading.Thread(target=receiver_thread_func)
Q1receiver_thread.start()




print(f"\n\n\nvoy a cerrar threads y cositas\n\n\n\n")
Q1receiver_thread.join()
Q1sender_thread.join()
fileQ1.close()
s.close()
print(f"\n\n\nQ1: {counter} rows received\n\n\n\n")
