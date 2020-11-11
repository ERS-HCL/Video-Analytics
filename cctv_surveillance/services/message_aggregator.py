import os
import sys
import logging

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from common.kafka_client import KafkaCli
from common.appcommon import init_logger
from common.kafka_base_consumer import KafkaEndConsumer


class MessageAggregator(KafkaEndConsumer):
    def __init__(self):
        self.discovered_faces = set([])
        self.discovered_objects = set([])
        self.agg = {
            
        }
        super().__init__()
        
    
    def handle_msg(self, msg):
        if msg.raw_frame.movie_filename not in self.agg.keys():
            self.agg[msg.raw_frame.movie_filename] = {
            "faces": set([]),
            "objects": set([]),
        }

        aggs = self.agg[msg.raw_frame.movie_filename]
        aggs["faces"].update(msg.matched_faces)
        
        # sample contents of msg.objects: ["person: 0.99", "cat: 0.98"]
        # drop the confidense number & extract inly the objects i.e. person, cat etc
        objects = map(lambda x: x.split(":")[0], msg.objects)
        aggs["objects"].update(objects)

        logger.debug(f"aggregation: f{self.agg}")
        logger.debug(f"Num of faces: {len(aggs['faces'])}, Num of objects: {len(aggs['objects'])}")
        



if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside message-aggregator...----------------------------")
    MessageAggregator()
    