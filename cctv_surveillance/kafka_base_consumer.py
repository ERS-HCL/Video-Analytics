import os
import sys
import logging

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from kafka_client import KafkaImageCli
from cctv_surveillance.appcommon import init_logger

logger = init_logger(__file__)

class KafkaBaseConsumer:
    def __init__(self):
        self.env = self.get_environ()
        self.consume_kafka_topic(handler= self.handle_msg)
        

    def get_environ(self) -> dict:
        return {
            "kafka_endpt": os.environ.get("KAFKA_BROKER_URL", ""),
            "in_topic": os.environ.get("INPUT_TOPIC", ""),
            "out_topic": os.environ.get("OUTPUT_TOPIC", ""),
        }


    def get_kafka_cli(self, clitype):
        topic_mapping= {"producer": self.env["out_topic"], "consumer": self.env["in_topic"]} #todo: use enum instead of string
        assert clitype in topic_mapping, "incorrect kafka client requested. It has to be either producer or consumer"
        return KafkaImageCli(
            bootstrap_servers= [self.env["kafka_endpt"]],
            topic= topic_mapping[clitype],
            stop_iteration_timeout= sys.maxsize
        )    
    
    def handle_msg(self, msg):
        assert False, "kafka consumer has not implemented its handle_msg function!!!"


class KafkaStreamingConsumer(KafkaBaseConsumer):
    def __init__(self):
        super().__init__()

    def consume_kafka_topic(self, handler):
        kafkaConsumer = self.get_kafka_cli("consumer")
        kafkaConsumer.register_consumer()
        logger.debug("polling kafka topic now...")
        kafkaProducer = self.get_kafka_cli("producer")

        for m in kafkaConsumer.consumer:
            logger.debug("received message from Kafka") 
            for (status, outmsg) in handler(m.value):
                if status:
                    outmsg.update_timestamp()
                    kafkaProducer.send_message(outmsg)     


class KafkaEndConsumer(KafkaBaseConsumer):
    def __init__(self):
        super().__init__()

    def consume_kafka_topic(self, handler):
        kafkaConsumer = self.get_kafka_cli("consumer")
        kafkaConsumer.register_consumer()
        logger.debug("polling kafka topic now...")
        for m in kafkaConsumer.consumer:
            logger.debug("received message from Kafka") 
            handler(m.value)