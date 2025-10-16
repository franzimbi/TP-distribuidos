#!/usr/bin/env python3
import os
import time
from client import Client
from configparser import ConfigParser
import logging


def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SYSTEM_PORT', config["DEFAULT"]["SYSTEM_PORT"]))
        config_params["host"] = str(os.getenv('SYSTEM_HOST', config["DEFAULT"]["SYSTEM_HOST"]))
        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SYSTEM_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e} .Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting server")

    return config_params


def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def main():
    config_params = initialize_config()

    port = config_params["port"]
    host = config_params["host"]
    logging_level = config_params["logging_level"]
    listen_backlog = config_params["listen_backlog"]
    initialize_log(logging_level)

    input_dir = os.getenv('CSV_INPUT_DIR')
    output_dir = 'results'

    logging.debug(
        f"action: config | result: success | port: {port} | host: {host} | "
        f"listen_backlog: {listen_backlog} | logging_level: {logging_level} | "
        f"input_dir: {input_dir} | output_dir: {output_dir}"
    )

    this_client = Client(host, port)
    this_client.start(input_dir, output_dir)
    this_client.close()


if __name__ == "__main__":
    main()
