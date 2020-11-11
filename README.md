# Video-Analytics

[![Kafka](https://img.shields.io/badge/streaming_platform-kafka-black.svg?style=flat-square)](https://kafka.apache.org)

[![Docker Images](https://img.shields.io/badge/docker_images-confluent-orange.svg?style=flat-square)](https://github.com/confluentinc/cp-docker-images)

[![Python](https://img.shields.io/badge/python-3.5+-blue.svg?style=flat-square)](https://www.python.org)

Horizontaly Scalable, Distributed system to churn out video feeds &amp; infer analytics

  

## Goals

Intent of this project is to build a ***Minimalistic***, ***Horizontally Scalable***, ***Distributed*** system to churn out volumes of data & provide meaningful insights into the same.

The current use case we are pursuing is that of churning CCTV footage & provide a Search Interface to query something like:

 - How many Cars and Motor Bikes passed through given area in the given time period?
 - What all the visitors cars entered in the residential complex? How much time they were parked in?
- many more...

Answers to these queries may be useful for Security or finding patterns etc.
  

## Why this project?

One can find many solutions for solving problems related to Computer Vision & data analytics in-general. However, most of these solutions are either in the form of ***technology*** (e.g. training a good face recognition model) or ***full blown product***, which could be over-sophisticated.

Our goal is to build something that works, which is easily testable & has minimal number of moving components, which can be used as a base for developing a full-blown product.

  ## Non-Goals
  This project doesn't focus on the ***Technology*** aspect. i.e. we are not focusing on, how best we can develop a license-plate recognition system. Instead, we leverage all open-source code & rather focus on orchestrating them in a highly cohesive, loosely coupled way.

## System Requirements
This is currently tested on Ubuntu-Linux.
However, only thing it expects is to have ***docker*** & ***docker-compose*** installed. Since everything runs inside the container, it should work on Windows & Mac as well (barring the helper bash scripts), though I haven't tested it.
  

## How to run
1. Change directory to that of the app
```sh
cd cctv-surveillance
```
2. Spin up Kafka Cluster
```sh
docker-compose -f ../docker-compose-kafka.yml up -d
# This spins up Kafaka as well as services for visualizing & administrating Docker Containers (Portainer) and Kafka message (Kafdrop)
```
3. Set the environment. e.g. filesystem path where the exported cctv footage movie files can be picked up
This can be done by editing the .env file & running it...

Edit the ".env" file to update the environment variable to work with your machine.
```sh
source .env
```
 - Run the App, by spinning up all required services
```sh
docker-compose up -d
```
  
  ## Code Organisation
  
 - Though the current use-case is for churning out CCTV footage data, this project can be used to build any app that works on ***Stream of data***. Each app is mapped to separate folder e.g. cctv_surveillance.
 - Every app is simply a composition of multiple microservices, which communicate to each other through ***Kafka Message Broker***
 - The Kafka Messages are serialised with Protocol Buffer.
 - The CCTV Surveillance app is structured as follows:
 ```
Video Frames Stream |-
		    | motion-detector (A)
		    | object-detector (B)
		    | license-plate-detector (TODO)
		    | face-detector (C)
		    | face-matcher (D)
		    | logstash pipeline (E)
		    | ElasticSearch	
		    | Kibana (TODO) 			 
 ```
 ```
 A: motion-detector: Most of video frames from CCTV involves non-activity & can be thrown right-away. This service takes a diff btw current frame & baseline frame, to determine if there is an activity
 B: object-detector: Applies YOLO object detector to see what objects are seen in the frame e.g. human, car, cat etc.
 C: face-detector: If there is human detected, then try to detect the face
 D: face-matcher: If a human face is detected, then check if it matches with anyone in database

E: Logstash is simply *watching* the Kafka messages produced by various microservices, as well as their logs. It applies transformation pipeline & finally indexed them into Elasticsearch.
 ```
  
## Disclaimer

This code is maintained inside HCL Open Source repository under MIT license. This is not an official HCL ERS product
 

## Credits

 - Code for "Object Detection" and "Motion Detection" for CCTV is taken from following excellent tutorials by **Adrian Rosebrock**
	 - [Basic Motion Detection and Tracking using Python and OpenCV](https://www.pyimagesearch.com/2015/05/25/basic-motion-detection-and-tracking-with-python-and-opencv/)
	 - [YOLO Object Detection with OpenCV](https://www.pyimagesearch.com/2018/11/12/yolo-object-detection-with-opencv/)
- References
	- [Building a Streaming Fraud Detection System with Kafka and Python](https://florimond.dev/blog/articles/2018/09/building-a-streaming-fraud-detection-system-with-kafka-and-python/)
	- [How to install and use the python face recognition and detection library in Ubuntu 16.04](https://ourcodeworld.com/articles/read/841/how-to-install-and-use-the-python-face-recognition-and-detection-library-in-ubuntu-16-04)
  

