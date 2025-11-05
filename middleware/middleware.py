from abc import ABC, abstractmethod
import pika
import logging


class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddlewareDeleteError(Exception):
    pass


class MessageMiddleware(ABC):

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    # Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    # no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    @abstractmethod
    def stop_consuming(self):
        pass

    # Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def send(self, message):
        pass

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    @abstractmethod
    def close(self):
        pass

    # Se fuerza la eliminación remota de la cola o exchange.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
    @abstractmethod
    def delete(self):
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, exchange_name, route_keys, exchange_type, queue_name=None):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                                            heartbeat=300,  # 5 minutos de tolerancia
                                                                            blocked_connection_timeout=600,  # evita
                                                                            # cortar por bloqueos largos
                                                                            socket_timeout=600,  # lectura/escritura
                                                                            # más paciente
                                                                            connection_attempts=10,  # intenta
                                                                            # reconectarse varias veces
                                                                            retry_delay=5,  # espera 5s entre intentos
                                                                            ))
        self.channel = self.connection.channel()
        self.route_keys = route_keys
        self.exchange = exchange_name
        self.queue = queue_name

        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=exchange_type
        )

        if self.queue:
            self.channel.queue_declare(queue=self.queue, durable=False, exclusive=False, auto_delete=True, arguments={
                'x-max-length': 1000000,  # hasta 1 millón de mensajes
                'x-max-length-bytes': 1073741824,  # o 1 GB de mensajes
                'x-overflow': 'drop-head'  # descarta los más viejos si se llena
            })

    def start_consuming(self, on_message_callback):
        try:
            for route_key in self.route_keys:
                self.channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=route_key)

            self.channel.basic_consume(queue=self.queue, on_message_callback=on_message_callback, auto_ack=True)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        self.channel.basic_publish(exchange=self.exchange, routing_key=self.route_keys[0], body=message,
                                   properties=pika.BasicProperties(delivery_mode=1))

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def delete(self):
        try:
            # Si hay una cola declarada, la eliminamos
            if self.queue:
                self.channel.queue_delete(queue=self.queue)

            # También eliminamos el exchange asociado
            if self.exchange:
                self.channel.exchange_delete(exchange=self.exchange)

        except pika.exceptions.AMQPConnectionError as e:
            # Error de conexión (por ejemplo, RabbitMQ no está disponible)
            raise MessageMiddlewareDisconnectedError() from e

        except Exception as e:
            # Cualquier otro error (por ejemplo, la cola no existe, o canal cerrado)
            raise MessageMiddlewareDeleteError() from e


class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host,
            heartbeat=300,  # 5 minutos de tolerancia
            blocked_connection_timeout=600,  # evita cortar por bloqueos largos
            socket_timeout=600,  # lectura/escritura más paciente
            connection_attempts=10,  # intenta reconectarse varias veces
            retry_delay=5,  # espera 5s entre intentos
        ))
        self._channel = self._connection.channel()
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name, durable=False, auto_delete=True, arguments={
            'x-max-length': 1000000,  # hasta 1 millón de mensajes
            'x-max-length-bytes': 1073741824,  # o 1 GB de mensajes
            'x-overflow': 'drop-head'  # descarta los más viejos si se llena
        })
        logging.getLogger("pika").propagate = False

    def start_consuming(self, on_message_callback, *, auto_ack=False, prefetch_count=1):
        try:
            # if prefetch_count and prefetch_count > 0:
            # self._channel.basic_qos(prefetch_count=prefetch_count)

            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=on_message_callback,
                auto_ack=auto_ack
            )
            self._channel.start_consuming()
        except Exception:
            return
        finally:
            try:
                if self._channel and self._channel.is_open:
                    self._channel.close()
            except Exception:
                pass
            try:
                if self._connection and self._connection.is_open:
                    self._connection.close()
            except Exception:
                pass
            logging.debug("[Middleware] cerrado correctamente")

    def stop_consuming(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except (pika.exceptions.ConnectionClosed,
                pika.exceptions.StreamLostError,
                pika.exceptions.AMQPConnectionErrorm, AttributeError) as e:
            return
            # raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al detener consumo: {e}") from e

    def send(self, message):
        try:
            self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=message,
                                        properties=pika.BasicProperties(delivery_mode=1))
        except (pika.exceptions.ConnectionClosed,
                pika.exceptions.StreamLostError,
                pika.exceptions.AMQPConnectionError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}") from e

    def close(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            # raise MessageMiddlewareCloseError(f"Error al cerrar conexión: {e}") from e
            return
        finally:
            logging.debug("[Middleware] conexión cerrada manualmente")

    def delete(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.queue_delete(queue=self._queue_name)
        except (pika.exceptions.ConnectionClosed,
                pika.exceptions.StreamLostError,
                pika.exceptions.AMQPConnectionError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error al eliminar cola: {e}") from e
