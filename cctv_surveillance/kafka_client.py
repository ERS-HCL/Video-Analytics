from json import dumps, loads
import os
import logging
import pickle

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger("video_analytics")

class KafkaCli:
    def __init__(self, bootstrap_servers, topic, stop_iteration_timeout,
                 value_serializer, value_deserializer,
                 ):
        logger.info("Initializing KafkaCli with servers: {servers}".format(servers= bootstrap_servers))
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.create_topic(topic)
        self.value_serializer = value_serializer
        self.value_deserializer = value_deserializer
        self.stop_iteration_timeout = stop_iteration_timeout
        

    def create_topic(self, topic):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers= self.bootstrap_servers,
            )

            topic_list = []
            topic_list.append(
                NewTopic(name=topic, num_partitions=1, replication_factor=1))
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
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,  # make this to False, if we want to consume from begining
                                      group_id='my-group-1',
                                      value_deserializer=self.value_deserializer,
                                      bootstrap_servers=self.bootstrap_servers,
                                      # StopIteration if no message after time in millisec
                                      consumer_timeout_ms=self.stop_iteration_timeout
                                      )

    def consume_messages(self):
        for m in self.consumer:
            print(m.value)



class KafkaImageCli(KafkaCli):
    def __init__(self, bootstrap_servers, topic, stop_iteration_timeout):
        super(KafkaImageCli, self).__init__(bootstrap_servers,
                                            topic,
                                            stop_iteration_timeout,
                                            value_serializer=lambda m: pickle.dumps(m),
                                            value_deserializer=lambda m: pickle.loads(m)
                                            )
