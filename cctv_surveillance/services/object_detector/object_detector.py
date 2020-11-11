'''
CREDITS: This code is copied & then modified from:
Original Author: Adrian Rosebrock
Blog: YOLO Object Detection with OpenCV
URL: https://www.pyimagesearch.com/2018/11/12/yolo-object-detection-with-opencv/
'''


import os
import sys
import logging
import time
import random
import string

import cv2
import imutils
import numpy as np
import face_recognition

currdir = os.path.dirname(__file__)
sys.path.append(os.path.join(currdir,"..", ".."))
sys.path.append(os.path.join(currdir,".."))

from common.kafka_client import KafkaCli
from common.appcommon import init_logger, save_image_data_to_jpg, ensure_dir_path

from common.kafka_base_consumer import KafkaStreamingConsumer


OUTDIR = "/usr/app/out/ObjectDetector"

class ObjectDetector(KafkaStreamingConsumer):
    def __init__(self):
        self.confidence = float(os.environ.get("CONFIDENCE", 0.5))
        self.threshold = float(os.environ.get("THRESHOLD", 0.3))
        super().__init__()


    def _get_frame_from_imagedata(self, imagedata):
        tempjpg = save_image_data_to_jpg(imagedata, "/usr/app/temp")  #todo: remove hardcoded path
        frame = face_recognition.load_image_file(tempjpg)  #todo: should read from in-memory stream- rather than temp file
        os.remove(tempjpg)
        return frame    


    def detect_objects(self, image):   
        '''
        returns identified objects as "labels" and the "annotated image"
        ret val:  (lavels, image)
        '''

        # load the COCO class labels our YOLO model was trained on
        labelsPath = os.path.join(currdir, "coco.names")
        LABELS = open(labelsPath).read().strip().split("\n")

        # initialize a list of colors to represent each possible class label
        np.random.seed(42)
        COLORS = np.random.randint(0, 255, size=(len(LABELS), 3), dtype="uint8")

        # derive the paths to the YOLO weights and model configuration
        weightsPath = os.path.join(currdir, "yolov3.weights")
        configPath = os.path.join(currdir, "yolov3.cfg")

        # load our YOLO object detector trained on COCO dataset (80 classes)
        # and determine only the *output* layer names that we need from YOLO
        logger.info("loading YOLO from disk...")
        net = cv2.dnn.readNetFromDarknet(configPath, weightsPath)

        # load our input image and grab its spatial dimensions
        (H, W) = image.shape[:2]

        # determine only the *output* layer names that we need from YOLO
        ln = net.getLayerNames()
        ln = [ln[i[0] - 1] for i in net.getUnconnectedOutLayers()]


        # construct a blob from the input image and then perform a forward
        # pass of the YOLO object detector, giving us our bounding boxes and
        # associated probabilities
        blob = cv2.dnn.blobFromImage(image, 1 / 255.0, (416, 416),	swapRB=True, crop=False)
        net.setInput(blob)
        start = time.time()
        layerOutputs = net.forward(ln)
        end = time.time()

        # show timing information on YOLO
        logger.info("YOLO took {:.6f} seconds".format(end - start))

        # initialize our lists of detected bounding boxes, confidences, and
        # class IDs, respectively
        boxes = []
        confidences = []
        classIDs = []


        # loop over each of the layer outputs
        for output in layerOutputs:
            # loop over each of the detections
            for detection in output:
                # extract the class ID and confidence (i.e., probability) of
                # the current object detection
                scores = detection[5:]
                classID = np.argmax(scores)
                confidence = scores[classID]

                # filter out weak predictions by ensuring the detected
                # probability is greater than the minimum probability
                if confidence > self.confidence:
                    # scale the bounding box coordinates back relative to the
                    # size of the image, keeping in mind that YOLO actually
                    # returns the center (x, y)-coordinates of the bounding
                    # box followed by the boxes' width and height
                    box = detection[0:4] * np.array([W, H, W, H])
                    (centerX, centerY, width, height) = box.astype("int")

                    # use the center (x, y)-coordinates to derive the top and
                    # and left corner of the bounding box
                    x = int(centerX - (width / 2))
                    y = int(centerY - (height / 2))

                    # update our list of bounding box coordinates, confidences,
                    # and class IDs
                    boxes.append([x, y, int(width), int(height)])
                    confidences.append(float(confidence))
                    classIDs.append(classID)

        
        # apply non-maxima suppression to suppress weak, overlapping bounding boxes
        idxs = cv2.dnn.NMSBoxes(boxes, confidences, self.confidence,  self.threshold)
    
        # ensure at least one detection exists
        identified_objects = []
        if len(idxs) > 0:
            # loop over the indexes we are keeping
            for i in idxs.flatten():
                # extract the bounding box coordinates
                (x, y) = (boxes[i][0], boxes[i][1])
                (w, h) = (boxes[i][2], boxes[i][3])

                # draw a bounding box rectangle and label on the image
                color = [int(c) for c in COLORS[classIDs[i]]]
                cv2.rectangle(image, (x, y), (x + w, y + h), color, 2)
                text = "{}: {:.4f}".format(LABELS[classIDs[i]], confidences[i])
                identified_objects.append(text)
                cv2.putText(image, text, (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX,
                    0.5, color, 2)
                logger.debug(f"text: {text}")    

        
        return identified_objects, image


    def handle_msg(self, msg):   
        frame = self._get_frame_from_imagedata(msg.raw_frame.image_bytes)
        objects, anotated_image = self.detect_objects(frame)
        
        if objects:
            # write annotated frame to jpg file
            fname = f"{self._frameid}.jpg"
            outdir = os.path.join(OUTDIR, msg.raw_frame.movie_filename)
            ensure_dir_path(outdir)
            cv2.imwrite(os.path.join(outdir, fname), anotated_image)

            # update the kafka message
            msg.objects.extend(objects)
            yield (True, msg)     # forward the same frame for further processing, if the motion is detected


def test_for_object_detector():
    '''
    test the object detector by isolating it from Kafka
    '''
    KafkaStreamingConsumer.__init__ = lambda x: None
    detector = ObjectDetector()
    detector.handle_msg = lambda s,msg:  (True, None)  
   
    image = cv2.imread(os.path.join(currdir, "testimage.jpg"))
    newimage = detector.detect_objects(image)
    cv2.imwrite(r"yolo.jpg", newimage)



if __name__== "__main__":
    logger = init_logger(__file__)
    logger.debug("------------start: inside object-detector...----------------------------")
    ensure_dir_path(OUTDIR)
    ObjectDetector()