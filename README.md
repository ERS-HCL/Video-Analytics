# Video-Analytics

[![Kafka](https://img.shields.io/badge/streaming_platform-kafka-black.svg?style=flat-square)](https://kafka.apache.org)
[![Docker Images](https://img.shields.io/badge/docker_images-confluent-orange.svg?style=flat-square)](https://github.com/confluentinc/cp-docker-images)
[![Python](https://img.shields.io/badge/python-3.5+-blue.svg?style=flat-square)](https://www.python.org)

Horizontaly Scalable, Distributed system to churn out video feeds &amp; infer analytics

## References
- Face Detection library: https://ourcodeworld.com/articles/read/841/how-to-install-and-use-the-python-face-recognition-and-detection-library-in-ubuntu-16-04


## Usage
- Download YouTube Videos
```
sudo youtube-dl -F <url>   
sudo youtube-dl -f <id> url
```

- Delete earlier test message if required...
```
sudo docker exec -i -t -u root $(sudo docker ps | grep kafka_kafka | cut -d' ' -f1) /bin/bash   # take bash inside kafka container...

$KAFKA_HOME/bin/kafka-topics.sh --list  --bootstrap-server kafka:9092   # List the topics
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic test1  --bootstrap-server kafka:9092  # Delete a topic
$KAFKA_HOME/bin/kafka-topics.sh --create --partitions 4 --bootstrap-server kafka:9092 --topic test
$KAFKA_HOME/bin/kafka-console-consumer.sh --from-beginning --bootstrap-server kafka:9092 --topic=test  # create a command-line consumer
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic=test  # create a command-line producer
```

- Run Consumer
```
python3 video_consumer.py --knownfaces /home/manoj/Pictures/known_faces_empty --outpath /home/manoj/Pictures/out --kafkatopic testimages2 --kafka-endpt "localhost:9092"
```
- Run Streamer
```
python3 video_streamer.py --videofile "/home/manoj/Videos/YouTube/ShriyutGangadharTipre_ MarathiSerial.webm" --kafkatopic testimages2 --kafka-endpt "localhost:9092"
```

- regarding endpoint -kafka-endpt
```
Use "localhost:9092", if you are running scripts from outside docker container
Use "kafka:29092", if you are running scripts from within the docker container
```

- Run Tests
```
# Reads movie file from file system, streams to kafka & consumes from kafka
# One needs to edit the hardcoded movie file name inside tests.py to run on respective machine
python3 tests.py   
```

## Run linter & style checker
```
pip install autopep8
autopep8 --in-place --aggressive --aggressive <filename>
```

## Feature Backlog
- docker swarm