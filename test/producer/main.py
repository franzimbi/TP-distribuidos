import os
from middleware import MessageMiddlewareQueue

def main():
    producer_queue = os.getenv("QUEUE_PRODUCE")
    msg = os.getenv("MEG_SEND")

    producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer_queue)

    try:
        for i in range(10):
            producer.send(msg)
    except KeyboardInterrupt:
        producer.stop_consuming()
        producer.close()


if __name__ == "__main__":
    main()
