#!/usr/bin/env python3
import os
import logging
from aggregator_tpv import Aggregator
from counter import Counter

def get_env(name, *, required=False, default=None):
    val = os.environ.get(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise KeyError(f"Missing env: {name}")
    return val

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=get_env("LOGGING_LEVEL", default="INFO"),
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    
    consume = get_env("CONSUME_QUEUE", required=True)
    produce = get_env("PRODUCE_QUEUE", required=True)
    type    = get_env("TYPE", required=True).strip().lower()
    params = get_env("PARAMS")
    logging.debug(f"[{type}] {consume} -> {produce}")
    
    params_buffer = []
    for p in params.split(","):
        params_buffer.append(p)
        
    if type == "sum":
        worker = Aggregator(
            consume, produce,
            key_col       = params_buffer[0],
            value_col     = params_buffer[1],
            bucket_kind   = params_buffer[2], 
            bucket_name   = params_buffer[3],
            time_col      = params_buffer[4],
            out_value_name= params_buffer[5],
        )
    elif type == "counter":
        worker = Counter(
            consume, produce,
            key_columns = params_buffer[0],
            count_name  = params_buffer[2],
        )

   
    worker.start()


if __name__ == "__main__":
    main()
