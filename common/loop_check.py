import socket
import threading

HEALTH_PORT = 3030


def crear_skt_healthchecker():
    skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    skt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    skt.bind(('', HEALTH_PORT))
    skt.listen()
    return skt


def loop_healthchecker(skt, stop_event):
    while not stop_event.is_set():
        try:
            conn, addr = skt.accept()
            conn.close()
        except OSError:
            break


def shutdown(stop_event, thread, socket):
    stop_event.set()
    try:
        socket.close()
    except:
        pass
    thread.join(timeout=2)
