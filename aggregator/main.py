#!/usr/bin/env python3
import os
from aggregator_tpv import TPVBySemesterStore
from counter import Counter
from configparser import ConfigParser
import logging

def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["CONSUME_QUEUE"] = str(os.getenv('CONSUME_QUEUE', config["DEFAULT"]["CONSUME_QUEUE"]))
        config_params["PRODUCE_QUEUE"] = str(os.getenv('PRODUCE_QUEUE', config["DEFAULT"]["PRODUCE_QUEUE"]))
        config_params["AGGREGATOR_NAME"] = str(os.getenv('AGGREGATOR_NAME', config["DEFAULT"]["AGGREGATOR_NAME"]))

        
        config_params["listen_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SYSTEM_LISTEN_BACKLOG"]))
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

    consume = config_params["CONSUME_QUEUE"]
    produce = config_params["PRODUCE_QUEUE"]
    print(f"[AGGREGATOR] Escuchando en cola: {consume}, enviando a: {produce}")
    name = config_params["AGGREGATOR_NAME"]
    logging_level = config_params["logging_level"]
    listen_backlog = config_params["listen_backlog"]
    initialize_log(logging_level)
    logging.debug(f"action: config | result: success | consume: {consume} | produce: {produce}  | "
                  f"listen_backlog: {listen_backlog} | logging_level: {logging_level}")
    
    logging.info(f"[{name}] {consume} -> {produce}")

    if name == "tpv_by_semester_store":
        worker = TPVBySemesterStore(consume, produce)
    elif name == "counter":
        worker = Counter(consume, produce)   

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()

if __name__ == "__main__":
    main()
