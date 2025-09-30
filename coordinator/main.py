from coordinator import Coordinator
import logging
import os


def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.getLogger("pika").setLevel(logging.WARNING)


def main():
    num_nodes = int(os.getenv("NUM_NODES", "1"))
    consumer = str(os.getenv(f"CONSUME_QUEUE"))
    downstream_q = str(os.getenv("DOWNSTREAM_QUEUE", "downstream"))
    producers = []
    for i in range(1, num_nodes+1):
        aux = str(os.getenv(f"PRODUCE_QUEUE_{i}"))
        if not aux:
            logging.error(f"Environment variable PRODUCE_QUEUE_{i} not set")
            continue
        producers.append(aux)


    coordinator = Coordinator(num_nodes, consumer, producers, downstream_q)
    coordinator.start()


if __name__ == "__main__":
    main()