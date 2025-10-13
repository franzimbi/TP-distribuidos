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

        params = req("params")
        try:
            col_name, col_id = [p.strip() for p in params.split(",", 1)]
        except ValueError:
            raise ValueError("params debe tener formato 'COLUMN_NAME,COLUMN_ID'")

        is_last_join = os.getenv("is_last_join").strip().lower() in ("1", "true", "yes")
        confirmation_queue = os.getenv("CONFIRMATION_QUEUE")
        logging_level  = os.getenv("LOGGING_LEVEL", "DEBUG")
        listen_backlog = int(os.getenv("SERVER_LISTEN_BACKLOG", "128"))

        return {
            "CONSUME_QUEUE": consume_q,
            "PRODUCE_QUEUE": produce_q,
            "CONFIRMATION_QUEUE": confirmation_queue,
            "JOIN_QUEUE": join_q,
            "COLUMN_NAME": col_name,
            "COLUMN_ID": col_id,
            "IS_LAST_JOIN": is_last_join,
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

    join_queue = config_params["JOIN_QUEUE"]

    logging_level = config_params["logging_level"]
    listen_backlog = config_params["listen_backlog"]
    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | queue_consumer: {queue_consumer} | queue_producer: {queue_producer} | "
        f"join_queue:{join_queue} | listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    this_join = Join(config_params["CONFIRMATION_QUEUE"], join_queue, config_params["COLUMN_ID"], config_params["COLUMN_NAME"], config_params["IS_LAST_JOIN"])
    this_join.start(queue_consumer, queue_producer)


if __name__ == "__main__":
    main()
