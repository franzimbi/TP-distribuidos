#!/usr/bin/env python3
import os
from aggregator_tpv import TPVBySemesterStore
import logging

def main():
    consume = os.getenv("CONSUME_QUEUE", "q32")
    produce = os.getenv("PRODUCE_QUEUE", "q33")
    print(f"[AGGREGATOR] Escuchando en cola: {consume}, enviando a: {produce}")
    name = os.getenv("AGGREGATOR_NAME", "tpv_by_semester_store")
    logging.info(f"[{name}] {consume} -> {produce}")
    agg = TPVBySemesterStore(consume, produce)
    try:
        agg.start()
    except KeyboardInterrupt:
        agg.stop()

if __name__ == "__main__":
    main()
