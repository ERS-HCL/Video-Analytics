#!/bin/bash

echo "Make sure you are using version 3.5.x of protobuf"
echo "later version of protobuf conflicts with the one used by logstash image"
echo "Current version of protoc:"
echo `protoc --version`

# Python output
protoc --python_out=. ./kafka_message.proto

# Ruby output: This is required for Logstash
protoc --ruby_out=. ./kafka_message.proto

# Create Descriptor files. This will shared with KafDrop service, which has UI for Kafka topics & messages
protoc --descriptor_set_out=cctv.desc ./kafka_message.proto

echo "... Done"