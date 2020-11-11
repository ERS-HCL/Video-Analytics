import os
import sys
import logging

import face_recognition

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from common.appcommon import init_logger, save_image_data_to_jpg
from common.kafka_base_consumer import KafkaStreamingConsumer

class FaceDetector(KafkaStreamingConsumer):
    def __init__(self ):
        super().__init__()


    def detect_face(self, imagedata):
        tempjpg = save_image_data_to_jpg(imagedata, "/tmp")
        image = face_recognition.load_image_file(tempjpg)  #todo: should read from in-memory stream- rather than temp file
        face_encodings = face_recognition.face_encodings(image)  # get encodings for all detected faces
        os.remove(tempjpg)
        return face_encodings

        
    def update_out_msg(self, msg, face_encodings):
        # convert numpy.ndarray to bytes, which is required by the protobuf data structure
        faces_encods_bytes = map(lambda e: e.tobytes(), face_encodings)  
        msg.faces.extend(list(faces_encods_bytes))
        return msg    


    def handle_msg(self, msg):
        face_encodings= self.detect_face(msg.raw_frame.image_bytes)
        if face_encodings:
            logger.debug("detected a face... sending to kafka topic...")
            msg = self.update_out_msg(msg, face_encodings)
            yield True, msg

   

if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside face_detector...----------------------------")
    FaceDetector()