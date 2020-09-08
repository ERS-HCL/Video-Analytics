import os
import sys
import logging

from appcommon import init_logger

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from kafka_client import KafkaImageCli

logger = init_logger(__file__)

class KafkaProducer:
    def __init__(self):
        self.kafka_endpt= os.environ.get("KAFKA_BROKER_URL", "")
        assert self.kafka_endpt
        self.topic= os.environ.get("TRANSACTIONS_TOPIC", "")
        self.stop_iteration_timeout = 3000
        self.kafkaCli = KafkaImageCli(
            bootstrap_servers= [self.kafka_endpt],
            topic= self.topic,
            stop_iteration_timeout= self.stop_iteration_timeout
        )


    def send_message(self, msg):
        self.kafkaCli.send_message(msg)


