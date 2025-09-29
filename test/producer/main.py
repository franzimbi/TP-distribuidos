import os
from middleware import MessageMiddlewareQueue

def main():
    producer_queue = os.getenv("QUEUE_PRODUCE")
    msg = os.getenv("MEG_SEND")
    cant = int(os.getenv("CANT_MSG"))

    producer = MessageMiddlewareQueue(host="rabbitmq", queue_name=producer_queue)

    try:
        for i in range(cant):
            producer.send(msg)
    except KeyboardInterrupt:
        producer.stop_consuming()
        producer.close()


if __name__ == "__main__":
    main()
