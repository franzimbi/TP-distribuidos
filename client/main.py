#!/usr/bin/env python3
import os
from client import Client
from common.protocol import send_batches_from_csv, recv_batches_from_socket

HOST = os.getenv("DISTRIBUTOR_HOST")
PORT = int(os.getenv("DISTRIBUTOR_PORT"))

this_client = Client(HOST, PORT)
this_client.start('csvs_files/transactions', 'results/q1.csv')
this_client.close()
