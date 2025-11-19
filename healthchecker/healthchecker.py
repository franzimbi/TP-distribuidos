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
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        self._health_thread = None
        self._health_sock = None

        self.stop_event = threading.Event()


    def _handle_shutdown(self, sig, frame):
        self.stop()

    def start(self):
        self._health_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._health_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._health_sock.bind(('', self.port))
        self._health_sock.listen()

        def loop():
            while not self.stop_event.is_set():
                try:
                    conn, addr = self._health_sock.accept()
                except OSError as e:
                    continue
                conn.close()

        self._health_thread = threading.Thread(target=loop, daemon=True)
        self._health_thread.start()

        while not self.stop_event.is_set():
            time.sleep(0.5)
            for node in self.nodes:
                try:
                    con = socket.create_connection((node, self.port))
                    con.shutdown(socket.SHUT_RDWR)
                    con.close()
                except socket.error:
                    if not self.stop_event.is_set():
                        self.revive_node(node)
                    else:
                        return
    
    def revive_node(self, node):
        if self.stop_event.is_set():
            return
        try:
            result = subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if result.returncode != 0: logging.error(f" Error intentando revivir a {node}. Stderr {result.stderr.decode('utf-8')}")
            else: logging.info(f"Intentando revivir a {node}...\n\t\t\tpadre nuestro...\n\t\t\tque estas en el cielo...\n\t\t\testa vivo! sobrevivio! Salud!")

        except Exception as e:
            logging.error(f"Error {e} en revive node")

    def stop(self): 
        self.stop_event.set()
        if self._health_thread:
            self._health_thread.join(timeout=1)
        if self._health_sock:
            self._health_sock.shutdown(socket.SHUT_RDWR)
            self._health_sock.close()
        logging.info("Healthchecker detenido")