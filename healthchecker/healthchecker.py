import socket
import logging
import subprocess
import signal
import time
import threading

class Healthchecker:
    def __init__(self, port, nodes):
        self.port = port
        # self.host = host
        self.nodes = nodes
        logging.info(f"nodos a chequear: {self.nodes}")
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        self._health_thread = None
        self._health_sock = None

        self.stop_event = threading.Event()


    def _handle_shutdown(self, sig, frame):
        logging.info("Graceful exit")
        self.stop()

    def start(self):
        logging.info(f"Arrancando healthcheck")

        self._health_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._health_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._health_sock.bind(('', self.port))
        self._health_sock.listen()

        def loop():
            while not self.stop_event.is_set():
                conn, addr = self._health_sock.accept()
                conn.close()

        self._health_thread = threading.Thread(target=loop, daemon=True)
        self._health_thread.start()

        while not self.stop_event.is_set():
            time.sleep(0.5)
            for node in self.nodes:
                # logging.info(f"Verificando nodo {node}")
                try:
                    con = socket.create_connection((node, self.port))
                    # logging.info(f"{node} ta' bien")
                    con.shutdown(socket.SHUT_RDWR)
                    con.close()
                except socket.error:
                    logging.info(f"{node} murio, intentando revivir...(reza por su alma)")
                    self.revive_node(node)
    
    def revive_node(self, node):
        try:
            result = subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if result.returncode != 0: logging.error(f"Stderr {result.stderr.decode('utf-8')}") 
            else: logging.info(f"{node} esta vivo! sobrevivio! Salud!")

        except Exception as e:
            logging.error(f"Error {e} en revive node")

    def stop(self): 
        self.stop_event.set()
        if self._health_sock:
            self._health_sock.shutdown(socket.SHUT_RDWR)
            self._health_sock.close()
        logging.info("Healthchecker detenido")