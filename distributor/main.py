#!/usr/bin/env python3
from middleware.middleware import MessageMiddlewareQueue
from common.protocol import Batch
from common.protocol import recv_batches_from_socket
import os
import time
import socket
import threading

Q1queue_consumer = os.getenv("CONSUME_QUEUE_Q1")
Q1queue_producer = os.getenv("PRODUCE_QUEUE_Q1")
Q3queue_consumer = os.getenv("CONSUME_QUEUE_Q3")
Q3queue_producer = os.getenv("PRODUCE_QUEUE_Q3")
class Distributor:
    def __init__(self):
        self.number_of_clients = 0
        self.clients = {} #key: clinet_id, value: socket
        self.number_of_queries = 4 #por ahora fijo, con un metodo a futuro para agregar una queue nueva al diccionario de queries (ya se q no van a haber nuevas, pero esto lo hace  'elastico' y ta bueno y no cuesta mucho hacerlo)
        self.producer_queues = {} #key: query_id, value: queue
        self.consumer_queues = {} #key: query_id, value: queue

        self.producer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_producer)
        self.producer_queues[2] = None # !!
        #self.producer_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_producer) #comento las de q3 por ahora para q no jodan
        self.producer_queues[4] = None # !!

        self.consumer_queues[1] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q1queue_consumer)
        self.consumer_queues[2] = None # !!
        #self.consumer_queues[3] = MessageMiddlewareQueue(host='rabbitmq', queue_name=Q3queue_consumer) #comento las de q3 por ahora para q no jodan
        self.consumer_queues[4] = None # !!

    def add_client(self, client_id, client_socket):
        self.number_of_clients += 1
        self.clients[client_id] = client_socket
        print(f"[DISTRIBUTOR] Cliente {client_id} conectado. Total clientes: {self.number_of_clients}")
    
    def add_query (): #a futuro, no importa ahora. si no les gusta, vuelenlo
        pass

    def distribute_batch_to_workers(self, query_id, batch):
        producer_queue = None
        if query_id == 1: #esto lo hago pq ahora mismo los batches no tienen flag de query_id, asi q lo pego con cinta para q lo arregle juanfran q a el le gusta arreglar cosas feas dice
            producer_queue = self.producer_queues[1]
        producer_queue.send(batch.encode())
        print(f"[DISTRIBUTOR] Batch distribuido a la cola de la query {query_id}.")
        
    def callback(self, ch, method, properties, body):
        batch = Batch.decode(body)
        print(f"[DISTRIBUTOR] Recibido batch de los workers.")
        #client_id = batch._header.get('client_id') # no existe client_id, hay q agregarlo cuando queramos multi-clientes
        client_id = 1 #por ahora fijo pq un solo cliente
        client_socket = self.clients.get(client_id)
        if client_socket:
            client_socket.sendall(body)
            print(f"[DISTRIBUTOR] Batch enviado al cliente {client_id}.")
        else:
            print(f"[DISTRIBUTOR] Cliente {client_id} no encontrado.")

    def start_consuming_from_workers(self, query_id):
        consumer_queue = self.consumer_queues.get(query_id)
        if consumer_queue is not None:
            consumer_queue.start_consuming(self.callback)
    
    def stop_consuming_from_all_workers(self):
        for query_id, consumer_queue in self.consumer_queues.items():
            if consumer_queue is not None:
                consumer_queue.stop_consuming()
                print(f"[DISTRIBUTOR] Detenido consumo de la query {query_id}.")

# INICIO DEL PROGRAMA PRINCIPAL #
distributor = Distributor() #ojo esto es global

TCP_PORT = int(os.getenv("TCP_PORT", 5000))  # default 5000 si no está
HOST = "0.0.0.0"

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, TCP_PORT))
server_socket.listen()

print(f"Distributor escuchando en {HOST}:{TCP_PORT}")

def handle_client(socket, addr):
    print(f"Cliente conectado: {addr}")
    distributor.add_client(addr, socket)

    try:
        # Supongamos que query_id = 1, podés adaptarlo según necesites
        query_id = 1

        for batch in recv_batches_from_socket(socket):
            distributor.distribute_batch_to_workers(query_id, batch)

            # Si es el último batch, salir del loop
            if batch.is_last_batch():
                print(f"Último batch recibido de {addr}")
                break

    except Exception as e: #muy generico, los profes se van a quejar
        print(f"Error con cliente {addr}: {e}")
    finally:
        socket.close()
        print(f"Cliente desconectado: {addr}")

def accept_clients():
    clients_threads = []
    client_number = 0
    while True:
        socket, addr = server_socket.accept()
        clients_threads.append(threading.Thread(target=handle_client, args=(socket, addr)))
        clients_threads[client_number].start()
        client_number += 1
        ##aca chequear si hay algun thread para liberar, ya no quiero pensar esto, ahora me da paja

Q1consumer_thread = threading.Thread(target=distributor.start_consuming_from_workers, args=(1,))
Q1consumer_thread.start()

accept_thread = threading.Thread(target=accept_clients)
accept_thread.start()

Q1consumer_thread.join()
accept_thread.join()
distributor.stop_consuming_from_all_workers()