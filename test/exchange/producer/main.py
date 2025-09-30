import os
import logging
from middleware.middleware import MessageMiddlewareExchange

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def main():
    host = os.getenv("RABBIT_HOST", "rabbitmq")
    exchange = os.getenv("EXCHANGE", "ex_test_1to1")
    exchange_type = os.getenv("EXCHANGE_TYPE", "direct")
    rk = os.getenv("ROUTING_KEY", "rk.1to1")
    msg = os.getenv("MEG_SEND", "exchange 1 a 1 OK")
    cant = int(os.getenv("CANT_MSG", "3"))
    queue_name = os.getenv("QUEUE_NAME")

    producer = MessageMiddlewareExchange(
        host=host,
        exchange_name=exchange,
        route_keys=[rk],
        exchange_type=exchange_type,
        queue_name=queue_name
    )

    try:
        for i in range(cant):
            producer.send(msg)
            log.info(f"Sent #{i+1}: {msg}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
