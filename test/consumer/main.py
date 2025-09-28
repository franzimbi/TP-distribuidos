import os
from middleware import MessageMiddlewareQueue
import logging

def main():
    producer_queue = os.getenv("QUEUE_PRODUCE")
    msg = os.getenv("MEG_SEND")

    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    logger = logging.getLogger(__name__)


    def callback(ch, method, properties, message):
        received = message.decode("utf-8")
        success = received == msg

        if success:
            # Verde claro si coincide
            logger.info(f"\033[92mReceived {received}: {success}\033[0m")
        else:
            # Rojo si falla
            logger.info(f"\033[91mReceived {received}: {success}\033[0m")

    producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer_queue)

    try:
        producer.start_consuming(callback)
    except KeyboardInterrupt:
        producer.stop_consuming()
        producer.close()


if __name__ == "__main__":
    main()
