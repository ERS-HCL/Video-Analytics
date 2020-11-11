'''
CREDITS: This code is copied & then modified from:
Original Author: Adrian Rosebrock
Blog: Basic Motion detection and tracking with Python and OpenCV
URL: https://www.pyimagesearch.com/2015/05/25/basic-motion-detection-and-tracking-with-python-and-opencv/
'''

import os
import sys
import logging
import datetime
import random
import string

import cv2
import imutils
import face_recognition

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from common.kafka_client import KafkaCli
from common.appcommon import init_logger, save_image_data_to_jpg, ensure_dir_path

from common.kafka_base_consumer import KafkaStreamingConsumer
import protobuf.kafka_message_pb2 as KafkaMsg

OUTDIR = "/usr/app/out/MotionDetector"


class MotionDetector(KafkaStreamingConsumer):
    def __init__(self):
        #todo: move following variables to docker-compose as env
        self.avg = {}
        self.min_area = 12000
        self.delta_thresh = 5
        self.frame_id = 0
        super().__init__()


    def _get_frame_from_imagedata(self, imagedata):
        tempjpg = save_image_data_to_jpg(imagedata, "/usr/app/temp")  #todo: remove hardcoded path
        frame = face_recognition.load_image_file(tempjpg)  #todo: should read from in-memory stream- rather than temp file
        os.remove(tempjpg)
        return frame


    def _write_frame_to_file(self,frame, msg):
        outdir = os.path.join(OUTDIR, msg.raw_frame.movie_filename)
        ensure_dir_path(outdir)
        outfile= os.path.join(outdir, f"{self._frameid}.jpg") 
        cv2.imwrite(outfile,frame)
        


    def detect_motion(self, msg) -> bool:
        '''
        returns True if motion is detected w.r.t. baseline frmes, otherwise False
        '''
        imagedata =  msg.raw_frame.image_bytes
        moviefile = msg.raw_frame.movie_filename
        self.frame_id += 1
        logger.debug(f"working on frame: {self.frame_id}, movie file: {moviefile}")

        frame = self._get_frame_from_imagedata(imagedata)

        # resize the frame, convert it to grayscale, and blur it
        frame = imutils.resize(frame, width=500)
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (21, 21), 0)

        # if the average frame is None, initialize it
        if not moviefile in self.avg:
            self.avg[moviefile] = gray.copy().astype("float")
            return False

        # accumulate the weighted average between the current frame and
        # previous frames, then compute the difference between the current
        # frame and running average
        cv2.accumulateWeighted(gray, self.avg[moviefile], 0.5)
        frameDelta = cv2.absdiff(gray, cv2.convertScaleAbs(self.avg[moviefile]))

        # threshold the delta image, dilate the thresholded image to fill
        # in holes, then find contours on thresholded image
        thresh = cv2.threshold(frameDelta, self.delta_thresh, 255, cv2.THRESH_BINARY)[1]
        thresh = cv2.dilate(thresh, None, iterations=2)
        cnts = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = imutils.grab_contours(cnts)

        max_contour_area= 0
        for c in cnts:
            # if the contour is too small, ignore it
            contour_area = cv2.contourArea(c)
            if contour_area < self.min_area:
                continue
            logger.debug(f"detected contour of size: {contour_area}")
            
            if contour_area > max_contour_area:
                max_contour_area = contour_area
            
            # compute the bounding box for the contour, draw it on the frame,
            # and update the text
            (x, y, w, h) = cv2.boundingRect(c)
            cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
            

        if max_contour_area < self.min_area:
            return False

        if len(cnts) > 0:
            self._write_frame_to_file(frame, msg)
            return True
        else:
            return False


    def handle_msg(self, msg):   
        motion_detected = self.detect_motion(msg)
        yield (motion_detected, msg)     # forward the same frame for further processing, if the motion is detected


if __name__== "__main__":
    logger = init_logger(__file__)
    ensure_dir_path(OUTDIR)
    logger.debug("------------start: inside motion-detector...----------------------------")
    MotionDetector()   