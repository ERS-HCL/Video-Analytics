from json import dumps, loads
import os
import logging
import pickle
import sys

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger("video_analytics")

class KafkaCli:
    def __init__(self, 
                 bootstrap_servers, 
                 topic
    ):
        logger.info("Initializing KafkaCli with servers: {servers}".format(servers= bootstrap_servers))
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.value_serializer=lambda m: pickle.dumps(m)
        self.value_deserializer=lambda m: pickle.loads(m)
        self.stop_iteration_timeout = sys.maxsize
        self.consumer_group_id = "1"

        self.create_topic(topic)
        

    def create_topic(self, topic):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers= self.bootstrap_servers,
            )

            topic_list = [(NewTopic(name=topic, num_partitions=1, replication_factor=1))]
            admin_client.create_topics(
                new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError:
            pass  # ignore


    def send_message(self, msg):
        producer = KafkaProducer(
            value_serializer=self.value_serializer,
            bootstrap_servers= self.bootstrap_servers
        )

        producer.send(self.topic, value=msg)
        producer.flush()


    def register_consumer(self):
        self.consumer = KafkaConsumer(self.topic,
                                      auto_offset_reset= 'earliest',
                                      enable_auto_commit= True,  # make this to False, if we want to consume from begining
                                      group_id= self.consumer_group_id,
                                      value_deserializer= self.value_deserializer,
                                      bootstrap_servers= self.bootstrap_servers,
                                      # StopIteration if no message after time in millisec
                                      consumer_timeout_ms=self.stop_iteration_timeout
                                      )

    def consume_messages(self):
        for m in self.consumer:
            print(m.value)

