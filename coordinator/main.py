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
    logging_level = os.getenv("logging_level")
    listen_backlog = os.getenv("listen_backlog") # ????? 
    initialize_log(logging_level)

    num_nodes = int(os.getenv("NUM_NODES"))
    consumer = str(os.getenv(f"QUEUE_CONSUME_FROM_NODES"))
    downstream_q = str(os.getenv("DOWNSTREAM_QUEUE", "downstream")) #TODO: hay mas de una, separar.
    producers = []
    produce_queue = str(os.getenv(f"QUEUES_PRODUCE_FOR_NODES"))
    for queue_name in produce_queue.split(','):
        producers.append(queue_name)

    coordinator = Coordinator(num_nodes, consumer, producers, downstream_q)
    coordinator.start()


if __name__ == "__main__":
    main()