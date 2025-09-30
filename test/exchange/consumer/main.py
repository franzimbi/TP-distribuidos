import os
import logging
import time
from middleware.middleware import MessageMiddlewareExchange

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def main():
    host = os.getenv("RABBIT_HOST", "rabbitmq")
    exchange = os.getenv("EXCHANGE", "ex_test_1to1")
    exchange_type = os.getenv("EXCHANGE_TYPE", "direct")
    rk = os.getenv("ROUTING_KEY", "rk.1to1")
    expected = os.getenv("EXPECTED", "exchange 1 a 1 OK")
    max_msg = int(os.getenv("MAX_MSG", "3"))
    queue_name = os.getenv("QUEUE_NAME")

    consumer = MessageMiddlewareExchange(
        host=host,
        exchange_name=exchange,
        route_keys=[rk],
        exchange_type=exchange_type,
        queue_name=queue_name
    )

    count = 0
    def callback(ch, method, properties, body):
        nonlocal count
        count += 1
        received = body.decode("utf-8")
        ok = (received == expected)
        color = "\033[92m" if ok else "\033[91m"
        log.info(f"{color}Received #{count}: {received} | match={ok}\033[0m")
        if count >= max_msg:
            consumer.stop_consuming()

    try:
        consumer.start_consuming(callback)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
