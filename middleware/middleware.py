from abc import ABC, abstractmethod
import pika


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
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.route_keys = route_keys
        self.exchange = exchange_name
        self.queue = queue_name

        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=exchange_type,
            durable=True
        )

        if self.queue:
            self.channel.queue_declare(queue=self.queue, durable=True, exclusive=False, auto_delete=False)

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
                                   properties=pika.BasicProperties(delivery_mode=2))

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def delete(self):
        pass

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=on_message_callback,
                auto_ack=True
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"\n\n\n \n\n\n BASTA DE CONSUMIR PORQUE SE CAYO LA CONEXION \n\n\n ")
            raise MessageMiddlewareDisconnectedError() from e

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=message,
                                    properties=pika.BasicProperties(delivery_mode=2))

    def close(self):
        try:
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def delete(self):
        pass
