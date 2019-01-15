import requests
import pika
from abc import ABC, abstractmethod

import logging
from . import utils
# consider https://pika.readthedocs.io/en/0.10.0/examples/asynchronous_consumer_example.html


class PikaConsumer(ABC):

    def __init__(self,
                 hostname,
                 port,
                 user,
                 password,
                 virtual_host,
                 queue_name
                 ):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.virtual_host = virtual_host
        self.queue_name = queue_name
        self.timer_id = None

    def consume(self):
        self.log("Initializing connection to " + str(self.hostname) + ":" + str(self.port))
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.hostname,
                                               self.port,
                                               self.virtual_host,
                                               credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.log("Connecting to queue " + self.queue_name)
        channel = self.connection.channel()
        channel.basic_qos(prefetch_count=1000)
        channel.basic_consume(self._wrap_on_message, self.queue_name)
        try:
            self.log("Starting to consume ... ")
            self.timer_id = self.connection.add_timeout(20, self.on_timeout)
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        self.connection.close()

    def on_timeout(self):
        # nothing
        pass

    def log(self, message):
        logger = logging.getLogger(__name__ + "_" + self.hostname)
        if isinstance(message, Exception):
            logger.error(message)
        else:
            logger.info(message)

    def _wrap_on_message(self, channel, method_frame, header_frame, body):
        self.on_message(channel, method_frame, header_frame, body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        if self.timer_id is not None:
            self.connection.remove_timeout(self.timer_id)
            self.timer_id = self.connection.add_timeout(20, self.on_timeout)

    @abstractmethod
    def on_message(self, channel, method_frame, header_frame, body):
        pass
