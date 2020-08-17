import os
import sys
import logging
import pickle
import datetime
import random
import string

import cv2
import imutils
import face_recognition

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from kafka_client import KafkaImageCli
from generator.appcommon import init_logger, save_image_data_to_jpg

firstFrame = None
min_area = 500

def get_environ() -> dict:
    return {
        "kafka_endpt": os.environ.get("KAFKA_BROKER_URL", ""),
        "in_topic": os.environ.get("INPUT_TOPIC", ""),
        "out_topic": os.environ.get("OUTPUT_TOPIC", ""),
    }


def get_kafka_cli(clitype):
    topic_mapping= {"producer": env["out_topic"], "consumer": env["in_topic"]} #todo: use enum instead of string
    assert clitype in topic_mapping, "incorrect kafka client requested. It has to be either producer or consumer"
    return KafkaImageCli(
        bootstrap_servers= [env["kafka_endpt"]],
        topic= topic_mapping[clitype],
        stop_iteration_timeout= sys.maxsize
    )


def detect_motion(imagedata):
    tempjpg = save_image_data_to_jpg(imagedata, "/tmp")
    frame = face_recognition.load_image_file(tempjpg)  #todo: should read from in-memory stream- rather than temp file
    os.remove(tempjpg)

    # resize the frame, convert it to grayscale, and blur it
    frame = imutils.resize(frame, width=500)
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (21, 21), 0)

	# if the first frame is None, initialize it
    global firstFrame
    if firstFrame is None:
        firstFrame = gray
        logger.debug("grabbed the firstframe")
        return

    logger.debug("checking the next frame...")
	# compute the absolute difference between the current frame and
	# first frame
    frameDelta = cv2.absdiff(firstFrame, gray)
    thresh = cv2.threshold(frameDelta, 25, 255, cv2.THRESH_BINARY)[1]

	# dilate the thresholded image to fill in holes, then find contours
	# on thresholded image
    thresh = cv2.dilate(thresh, None, iterations=2)
    cnts = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = imutils.grab_contours(cnts)
    for c in cnts:
        logger.debug("got the contour")
        # if the contour is too small, ignore it
        if cv2.contourArea(c) < min_area:
            continue
        # compute the bounding box for the contour, draw it on the frame,
		# and update the text
        (x, y, w, h) = cv2.boundingRect(c)
        cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
        text = "Occupied"

    def get_random_filename(prefix):
        letters = ["unknown-"] +  [random.choice(string.ascii_lowercase) for i in range(5)]
        fname = "".join(letters)
        return f"{prefix}_{fname}.jpg"

    outfile= os.path.join("/tmp", get_random_filename(prefix= "motion"))
    cv2.imwrite(outfile,frame)
    if cnts:
        return True
    else:
        return False
        


def consume_kafka_topic():
    kafkaConsumer = get_kafka_cli("consumer")
    kafkaConsumer.register_consumer()
    logger.debug("polling kafka topic now...")
    kafkaProducer = get_kafka_cli("producer")

    for m in kafkaConsumer.consumer:
        logger.debug("received message from Kafka")
        frame = detect_motion(m.value)
        if frame:
            logger.debug("detected motion. Sending output to kafka...")
            kafkaProducer.send_message(m.value)
        

if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside motion-detector...----------------------------")
    env = get_environ()
    consume_kafka_topic()    