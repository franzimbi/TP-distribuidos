from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from common.batch import Batch


class Join:
    def __init__(self, join_queue, column_id, column_name, logger):
        self.producer_queue = None
        self.consumer_queue = None
        self.join_dictionary = {}
        self.column_id = column_id
        self.column_name = column_name
        self.join_queue = join_queue
        self.logger = logger
        # recibe de join_queue los datos y arma el diccionario de id-valor
        self.logger.info("action: receive_join_data | status: waiting")
        self.join_queue.start_consuming(self.callback_to_receive_join_data)
        self.logger.info("action: receive_join_data | status: finished | entries: %d", len(self.join_dictionary))

    def callback_to_receive_join_data(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        print(batch)
        # id = batch.index_of(self.column_id)
        # name = batch.index_of(self.column_name)
        # for row in batch:
        #     if row[id] not in self.join_dictionary:
        #         self.join_dictionary[row[id]] = row[name]

        if batch.is_last_batch():
            self.join_queue.stop_consuming()

    def start(self, consumer, producer):
        self.consumer_queue = consumer
        self.producer_queue = producer
        self.consumer_queue.start_consuming(self.callback)

    def callback(self, ch, method, properties, message):
        batch = Batch()
        batch.decode(message)
        try:
            batch.change_header_name_value(self.column_id, self.column_name, self.join_dictionary)
            self.logger.debug(f'action: join_batch_with_dicctionary | result: success')
        except (ValueError, KeyError) as e:
            self.logger.error(f'action: join_batch_with_dicctionary | result: fail | errror: {e}')
        self.producer_queue.send(batch.encode())
