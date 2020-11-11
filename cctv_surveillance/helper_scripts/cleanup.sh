#!/bin/bash

# Stop all services
docker-compose stop

# Remove kafka topics related to our services (clean start)
docker-compose -f ../docker-compose-kafka.yml exec kafka bash ./helper_scripts/remove_all_topics.sh

# Clear prev logs in containers
#sudo bash ./clear_logs.sh
docker-compose rm -f

