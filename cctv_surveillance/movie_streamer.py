import os
import sys
import logging
import time
from glob import glob

import cv2

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,".."))

from kafka_client import KafkaImageCli
from cctv_surveillance.appcommon import init_logger
from kafka_producer import KafkaProducer
from framedata import FrameData


class MovieStreamer(KafkaProducer):
    def __init__(self):
        self.movie_source= os.environ.get("MOVIE_FILES_SOURCE", "")
        self.frame_cap_period= float(os.environ.get("FRAME_CAPTURE_PERIOD", ""))
        assert os.path.exists(self.movie_source), f"filepath does not exist: {self.movie_source}"
        super().__init__()
        self.stream_movies()


    def get_movie_files(self) -> list:
        videofiles = glob(f'{self.movie_source}/*.webm')
        videofiles.extend(glob(f'{self.movie_source}/*.mp4'))
        return [os.path.join(self.movie_source, f) for f in videofiles]


    def read_movie(self, moviefile):
        logger.debug(f"Reading movie file: {moviefile}")
        video = cv2.VideoCapture(moviefile)
        
        totalframes= video.get(cv2.CAP_PROP_FRAME_COUNT)
        logger.debug(f"Total # of frames: {totalframes}")
        fps= video.get(cv2.CAP_PROP_FPS)
        logger.debug(f"fps: {fps}")
        logger.debug(f"Capturing frame every {self.frame_cap_period} seconds")

        frames_to_skip = int(fps * self.frame_cap_period)
        logger.debug(f"frames to skip: {frames_to_skip}")
        frameid = -1
        while(video.isOpened()):
            success, frame = video.read()
            if not success: 
                break

            frameid += 1 
            if frameid % frames_to_skip: 
                continue

            logger.debug(f"got frame id# {frameid} of {totalframes}, at approx {int(frameid/fps)} secs")    

            # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
            frame = frame[:, :, ::-1]    #TODO: This should be moved to consumer    

            ret, buffer = cv2.imencode('.jpg', frame)
            yield buffer
        video.release()  # Todo: Use Context Manager


    def stream_movies(self):
        logger.debug(f"streaming to kafka endpoint: {self.kafka_endpt}")
        
        st_time = time.time()
        for movie in self.get_movie_files():
            for frame in self.read_movie(movie):
                msg = FrameData()
                msg.raw_frame= frame.tobytes()
                self.send_message(msg)
        end_time = time.time()
        logger.debug(f"---------------- Done: In {(end_time-st_time)/60} minutes --------------------")
    
    

if __name__ == "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside movie_streamer...----------------------------")
    MovieStreamer()