#!/bin/bash

# to be run from within the Kafka container

echo "listing topics before deletion:"
$KAFKA_HOME/bin/kafka-topics.sh --list  --bootstrap-server kafka:9092

$KAFKA_HOME/bin/kafka-topics.sh --delete --topic cctv.raw-frames  --bootstrap-server kafka:9092
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic cctv.faces-detected  --bootstrap-server kafka:9092
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic cctv.matched-faces  --bootstrap-server kafka:9092
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic cctv.motion-detected  --bootstrap-server kafka:9092
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic cctv.object-detected  --bootstrap-server kafka:9092

echo "listing topics after deletion:"
$KAFKA_HOME/bin/kafka-topics.sh --list  --bootstrap-server kafka:9092