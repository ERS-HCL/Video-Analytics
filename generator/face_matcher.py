import os
import sys
import logging
import fnmatch
import pickle

import cv2
import face_recognition
from pipe import Pipe, select, where

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from kafka_client import KafkaImageCli
from generator.appcommon import init_logger, save_image_data_to_jpg

@Pipe
def tolist(iterable):
    return list(iterable)

@Pipe
def toSet(iterable):
    return set(iterable)


def get_environ() -> dict:
    return {
        "kafka_endpt": os.environ.get("KAFKA_BROKER_URL", ""),
        "in_topic": os.environ.get("INPUT_TOPIC", ""),
        "out_topic": os.environ.get("OUTPUT_TOPIC", ""),
        "face_database": os.environ.get("FACE_DATABASE", ""),
        "match_tol": float(os.environ.get("FACE_MATCH_TOL", 0.6)),
    }


def get_kafka_cli(clitype):
    topic_mapping= {"producer": env["out_topic"], "consumer": env["in_topic"]} #todo: use enum instead of string
    assert clitype in topic_mapping, "incorrect kafka client requested. It has to be either producer or consumer"
    return KafkaImageCli(
        bootstrap_servers= [env["kafka_endpt"]],
        topic= topic_mapping[clitype],
        stop_iteration_timeout= sys.maxsize
    )


def load_known_faces(known_faces_path):
    assert os.path.exists(known_faces_path)
    jpgfiles = fnmatch.filter(os.listdir(known_faces_path), "*.jpg")    # Treat filename-without-extn as image title
    image_titles = [os.path.splitext(f)[0] for f in jpgfiles]
    jpgfpaths = [os.path.join(known_faces_path, f) for f in jpgfiles]

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


def match_faces(new_face, known_faces, tol):
    knonwn_encodes = known_faces | select(lambda f: f["encod"]) | tolist
    faceencod = new_face["encod"]
    matches = face_recognition.compare_faces(knonwn_encodes, faceencod, tol)

    # Select only matched records
    return zip(matches, known_faces) \
        | where(lambda x: x[0])    \
        | select(lambda m: m[1])   \
        | tolist   



def consume_kafka_topic():
    known_faces = load_known_faces(env["face_database"])
    known_faces_names= known_faces | select(lambda f: f["name"]) | tolist
    logger.debug(f"known faces: {known_faces_names}")

    kafkaProducer = get_kafka_cli("producer")

    kafkaConsumer = get_kafka_cli("consumer")
    kafkaConsumer.register_consumer()
    logger.debug("polling kafka topic now...")

    for m in kafkaConsumer.consumer:
        logger.debug("received message from Kafka")
        new_face = pickle.loads(m.value)
        matches= match_faces(new_face, known_faces, env["match_tol"])
        if matches:
            titles= matches | select(lambda m: m["name"]) | tolist
            new_face["matches"]= titles
            outmsg= pickle.dumps(new_face)
            _ = pickle.loads(outmsg)
            logger.debug(f"diagnostic check: {_.keys()}")
            logger.debug(f"matches: {_['matches']}")
            kafkaProducer.send_message(outmsg)   
            
    logger.debug("ended kafka polling...")



if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside face_matcher...----------------------------")
    env = get_environ()
    logger.debug(f"environ: {env}")
    consume_kafka_topic()

