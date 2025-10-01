from configparser import ConfigParser
import logging
import os
from middleware.middleware import MessageMiddlewareQueue
from reducer import Reducer


def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["CONSUME_QUEUE"] = os.getenv('CONSUME_QUEUE', config["DEFAULT"]["CONSUME_QUEUE"])
        config_params["PRODUCE_QUEUE"] = os.getenv('PRODUCE_QUEUE', config["DEFAULT"]["PRODUCE_QUEUE"])

        config_params["TOP"] = int(os.getenv('TOP'))
        config_params["COLUMN_NAME"] = str(os.getenv('COLUMN_NAME'))

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
    top = config_params["TOP"]
    columns = config_params["COLUMN_NAME"]
    logging_level = config_params["logging_level"]
    listen_backlog = config_params["listen_backlog"]
    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | queue_consumer: {queue_consumer} | queue_producer: {queue_producer} | "
        f"top:{top} | listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
    producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)

    reducer = Reducer(consumer, producer, top, columns)

    reducer.start()

if __name__ == "__main__":
    main()
