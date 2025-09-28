from configparser import ConfigParser
import logging
import os
from middleware.middleware import MessageMiddlewareQueue
from join import Join


def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["CONSUME_QUEUE"] = os.getenv('CONSUME_QUEUE', config["DEFAULT"]["CONSUME_QUEUE"])
        config_params["PRODUCE_QUEUE"] = os.getenv('PRODUCE_QUEUE', config["DEFAULT"]["PRODUCE_QUEUE"])
        config_params["JOIN_QUEUE"] = os.getenv('JOIN_QUEUE', config["DEFAULT"]["JOIN_QUEUE"])

        config_params["COLUMN_ID"] = os.getenv('COLUMN_ID', config["DEFAULT"]["COLUMN_ID"])
        config_params["COLUMN_NAME"] = os.getenv('COLUMN_NAME', config["DEFAULT"]["COLUMN_NAME"])

        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SYSTEM_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
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

    consumer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_consumer)
    producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=queue_producer)
    join_data = MessageMiddlewareQueue(host="rabbitmq", queue_name=join_queue)

    this_join = Join(join_data, config_params["COLUMN_ID"], config_params["COLUMN_NAME"], logging)

    this_join.start(consumer, producer)


if __name__ == "__main__":
    main()
