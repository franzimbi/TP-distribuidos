from configparser import ConfigParser
import logging
import os
from middleware.middleware import MessageMiddlewareQueue
from join import Join
import signal
import sys


def initialize_config():
    import os

    def req(name: str) -> str:
        v = os.getenv(name)
        if v is None or str(v).strip() == "":
            raise KeyError(name)
        return v

    try:
        consume_q       = req("queueEntradaData")
        produce_q       = req("queuesSalida")
        join_q          = req("queueEntradaJoin")
        exchange_name   = req("entradaJoin")
        try:
            entrada_type, name_e = [p.strip() for p in exchange_name.split(",", 1)]
        except ValueError:
            raise ValueError("entradaJoin debe tener formato 'exchange_type,exchange_name'")

        coord_consume_q = req("queue_to_receive_coordinator")
        coord_produce_q = req("queue_to_send_coordinator")

        params = req("params")
        try:
            col_name, col_id = [p.strip() for p in params.split(",", 1)]
        except ValueError:
            raise ValueError("params debe tener formato 'COLUMN_NAME,COLUMN_ID'")

        use_disk = os.getenv("use_diskcache", "False").strip().lower() in ("1", "true", "yes")

        logging_level  = os.getenv("LOGGING_LEVEL", "INFO")
        listen_backlog = int(os.getenv("SERVER_LISTEN_BACKLOG", "128"))

        return {
            "CONSUME_QUEUE": consume_q,
            "PRODUCE_QUEUE": produce_q,
            "JOIN_QUEUE": join_q,
            "EXCHANGE_NAME": name_e,
            "COORDINATOR_CONSUME_QUEUE": coord_consume_q,
            "COORDINATOR_PRODUCE_QUEUE": coord_produce_q,
            "COLUMN_NAME": col_name,
            "COLUMN_ID": col_id,
            "USE_DISKCACHE": use_disk,
            "listen_backlog": listen_backlog,
            "logging_level": logging_level,
        }

    except KeyError as e:
        raise KeyError(f"Key was not found. Error: '{e.args[0]}' .Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting server")

def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger("pika").setLevel(logging.CRITICAL)

def main():
    print("Starting Joinner...")
    config_params = initialize_config()

    queue_consumer = config_params["CONSUME_QUEUE"]
    queue_producer = config_params["PRODUCE_QUEUE"]
    coordinator_consumer = config_params["COORDINATOR_CONSUME_QUEUE"]
    coordinator_producer = config_params["COORDINATOR_PRODUCE_QUEUE"]

    join_queue = config_params["JOIN_QUEUE"]
    exchange_name = config_params["EXCHANGE_NAME"]

    logging_level = config_params["logging_level"]
    listen_backlog = config_params["listen_backlog"]
    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | queue_consumer: {queue_consumer} | queue_producer: {queue_producer} | "
        f"join_queue:{join_queue} | listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    this_join = Join(exchange_name, join_queue, config_params["COLUMN_ID"], config_params["COLUMN_NAME"], config_params["USE_DISKCACHE"])
    this_join.start(queue_consumer, queue_producer, coordinator_consumer, coordinator_producer)


if __name__ == "__main__":
    main()
