#!/bin/bash

SERVICE_NAME=$1

echo "stopping & removing the TEST container"
docker stop TEST
docker rm TEST

echo "starting the TEST container with TEST_MODE"
docker-compose run -d -e TEST_MODE=1 --name TEST $SERVICE_NAME

echo "run following command to see logs: sudo docker logs TEST"

