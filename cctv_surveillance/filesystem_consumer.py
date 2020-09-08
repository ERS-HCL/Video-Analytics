import os
import sys
import logging

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from kafka_client import KafkaImageCli
from cctv_surveillance.appcommon import init_logger, save_image_data_to_jpg
from framedata import FrameData

from kafka_base_consumer import KafkaEndConsumer


class FilesystemConsumer(KafkaEndConsumer):
    def __init__(self):
        self.discovered = set([])
        self.out_fileloc = os.environ.get("OUTPUT_FILE_LOCATION", "")
        super().__init__()
        
    
    def handle_msg(self, msg):
        matches= set(msg.matched_faces)
        if matches.difference(self.discovered):     # new matches discovered    
            self.discovered = self.discovered.union(set(matches))
            save_image_data_to_jpg(msg.raw_frame, self.out_fileloc)  # save this image somewhere for ref
            logger.debug(f"frame processing time = {msg.t_updated - msg.t_created}")
            logger.debug(f"discovered so far... {self.discovered}")



if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside filesystem-consumer...----------------------------")
    FilesystemConsumer()
    