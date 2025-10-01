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
    name    = get_env("AGGREGATOR_NAME", required=True).strip().lower()

    logging.info(f"[{name}] {consume} -> {produce}")

    if name == "sum":
        worker = Aggregator(
            consume, produce,
            key_col       = get_env("KEY_COLUMN",   required=True),
            value_col     = get_env("VALUE_COLUMN", required=True),
            bucket_kind   = get_env("BUCKET_KIND",  required=True), 
            bucket_name   = get_env("BUCKET_NAME",  required=True),
            time_col      = get_env("TIME_COL",     default="created_at"),
            out_value_name= get_env("OUT_VALUE",    default="total"),
        )
    elif name == "counter":
        worker = Counter(
            consume, produce,
            key_columns = get_env("KEY_COLUMNS", required=True),
            count_name  = get_env("COUNT_NAME",  required=True),
        )

   
    worker.start()


if __name__ == "__main__":
    main()
