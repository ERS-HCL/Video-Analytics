from json import dumps, loads
import os
import logging
import pickle
import sys

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

#todo: a dependency issue, which needs to be fixed
import protobuf.kafka_message_pb2 as KafkaMsg  

logger = logging.getLogger("video_analytics")

class KafkaCli:
    def __init__(self, 
                 bootstrap_servers, 
                 topic
    ):
        def deserialize_msg(m):
            # instantiate empty message
            msg = KafkaMsg.Frame()
            raw_frame = KafkaMsg.Frame.RawFrame()
            msg.raw_frame.CopyFrom(raw_frame)

            # deserialize
            msg.ParseFromString(m)
            return msg

        logger.info("Initializing KafkaCli with servers: {servers}".format(servers= bootstrap_servers))
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.value_serializer= lambda m: m.SerializeToString()
        self.value_deserializer= lambda m: deserialize_msg(m)
       
        self.stop_iteration_timeout = sys.maxsize
        self.consumer_group_id = "1"

        self.create_topic(topic)
        self.group_id_for_testing = "testing"
        

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


    def send_message(self, value, key):
        producer = KafkaProducer(
            value_serializer= self.value_serializer,
            key_serializer= str.encode if key else None,
            bootstrap_servers= self.bootstrap_servers
        )

        producer.send(self.topic, key=key, value=value)
        producer.flush()


    def register_consumer(self):
        test_mode= bool(os.environ.get("TEST_MODE",False))
        logger.debug(f"TEST_MODE= {test_mode}")
        print(f"TEST_MODE= {test_mode}")
        group_id = self.group_id_for_testing if test_mode else self.consumer_group_id
        enable_auto_commit = False if test_mode else True
        
        self.consumer = KafkaConsumer(self.topic,
                                      auto_offset_reset= 'earliest',
                                      enable_auto_commit= enable_auto_commit,  # make this to False, if we want to consume from begining
                                      group_id= group_id,
                                      value_deserializer= self.value_deserializer,
                                      key_deserializer= lambda k: k.decode() if k else None,
                                      bootstrap_servers= self.bootstrap_servers,
                                      # StopIteration if no message after time in millisec
                                      consumer_timeout_ms=self.stop_iteration_timeout
                        )

    def consume_messages(self):
        for m in self.consumer:
            print(m.value)

