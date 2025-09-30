from configparser import ConfigParser
import logging
import os
from middleware.middleware import MessageMiddlewareQueue
from join import Join
import signal


def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["CONSUME_QUEUE"] = os.getenv('CONSUME_QUEUE', config["DEFAULT"]["CONSUME_QUEUE"])
        config_params["PRODUCE_QUEUE"] = os.getenv('PRODUCE_QUEUE', config["DEFAULT"]["PRODUCE_QUEUE"])
        config_params["JOIN_QUEUE"] = os.getenv('JOIN_QUEUE', config["DEFAULT"]["JOIN_QUEUE"])

        config_params["COLUMN_ID"] = os.getenv('COLUMN_ID', config["DEFAULT"]["COLUMN_ID"])
        config_params["COLUMN_NAME"] = os.getenv('COLUMN_NAME', config["DEFAULT"]["COLUMN_NAME"])

        config_params["USE_DISKCACHE"] = os.getenv('USE_DISKCACHE', 'False').strip().lower() in ('1', 'true', 'yes')

        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SYSTEM_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger("pika").setLevel(logging.WARNING)

def main():
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

    this_join = Join(join_queue, config_params["COLUMN_ID"], config_params["COLUMN_NAME"], config_params["USE_DISKCACHE"])
    this_join.start(queue_consumer, queue_producer)


if __name__ == "__main__":
    main()
