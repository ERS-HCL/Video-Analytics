import os
import sys
import logging
import fnmatch

import cv2
import face_recognition
import numpy as np
from pipe import Pipe, select, where

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from common.kafka_client import KafkaCli
from common.appcommon import init_logger, save_image_data_to_jpg
from common.kafka_base_consumer import KafkaStreamingConsumer

@Pipe
def tolist(iterable):
    return list(iterable)

@Pipe
def toSet(iterable):
    return set(iterable)


class FaceMatcher(KafkaStreamingConsumer):
    def __init__(self):
        self.face_database = os.environ.get("FACE_DATABASE", "")
        self.match_tol = float(os.environ.get("FACE_MATCH_TOL", 0.6))
        
        self.known_faces = self.load_known_faces()
        known_faces_names= self.known_faces | select(lambda f: f["name"]) | tolist
        logger.debug(f"known faces: {known_faces_names}")
        
        super().__init__()


    def load_known_faces(self):
        assert os.path.exists(self.face_database)
        jpgfiles = fnmatch.filter(os.listdir(self.face_database), "*.jpg")    # Treat filename-without-extn as image title
        image_titles = [os.path.splitext(f)[0] for f in jpgfiles]
        jpgfpaths = [os.path.join(self.face_database, f) for f in jpgfiles]

        known_faces = []
        for title, fpath in zip(image_titles, jpgfpaths):
            image = face_recognition.load_image_file(fpath)
            face_encoding = face_recognition.face_encodings(image)[0]   # assumption: there is only one face per image file

            known_faces.append({
                "id": face_encoding.data.tobytes(),    # hash of the encoding matrix: to serve as primary key
                "encod": face_encoding,
                "name": title,
                "imgfile": fpath,
            })
        return known_faces


    def match_faces(self, faceencod, known_faces, tol):
        knonwn_encodes = known_faces | select(lambda f: f["encod"]) | tolist
        matches = face_recognition.compare_faces(knonwn_encodes, faceencod, tol)

        # Select only matched records
        return zip(matches, known_faces) \
            | where(lambda x: x[0])    \
            | select(lambda m: m[1])   \
            | tolist   


    def handle_msg(self, msg):
        matches = []
        for new_face in msg.faces:
            # Converting byte format back to NumPy array
            new_face = np.frombuffer(new_face)
            logger.debug(f"type of new_face: {type(new_face)}")
            matches.extend(self.match_faces(new_face, self.known_faces, self.match_tol))

        if matches:
            titles= matches | select(lambda m: m["name"]) | tolist
            msg.matched_faces.extend(titles)
            logger.debug(f"match found: {titles}")
            yield True, msg
        else:
            logger.debug("New face found. Updating the database...")
            save_image_data_to_jpg(msg.raw_frame.image_bytes, outpath= self.face_database)
            self.known_faces = self.load_known_faces()


if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside face_matcher...----------------------------")
    FaceMatcher()

