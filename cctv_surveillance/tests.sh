#!/bin/bash

echo "-----------------------------------------------"
echo "These tests are temporary drop-in arrangement, till robust tests are authored"
echo "the tests clean logs, topics from previous run, spin-up all services &"
echo "looks for expected output in the log statements - for the given input movie file"
echo "CAUTION: These tests are currently indeterministic due to arbirary sleep statements"
echo "-----------------------------------------------"


# Determine time (in secs) to wait for all services to be up & running
WAIT_FOR_SERVICES=$1
if ! [[ $WAIT_FOR_SERVICES == ?(-)+([0-9]) ]];
then
    echo -e "No or invalid argument for startup-time for services. Assuming it to be 30 secs"
    WAIT_FOR_SERVICES=30
fi
echo -e "test asserts will be run $WAIT_FOR_SERVICES seconds after services are started"


# Get logs from the particular service & look if the log contains a specific content that we are expecting
CheckService() {
    GREEN="\e[1;32m"
    RED="\e[1;31m"
    MAGENDA="\e[1;35m"
    RESET_COLOR="\e[0m"

    SERVICE=$1
    PATT=$2

    LOG_OUTPUT=`docker-compose logs $SERVICE | cut -d ':' -f 5- | sort | uniq `
    if [[ "$LOG_OUTPUT" == *"$PATT"* ]]; 
    then
        echo -e "Service $MAGENDA $SERVICE: $GREEN pass $RESET_COLOR"
    else
        echo -e "Service $MAGENDA $SERVICE: $RED fail $RESET_COLOR"
    fi
}


# Stop all services
docker-compose stop

# Remove kafka topics related to our services (clean start)
docker-compose -f ../docker-compose-kafka.yml exec kafka bash ./remove_all_topics.sh

# Clear prev logs in containers
#sudo bash ./clear-services-logs.sh
docker-compose rm -f


# Start the Services
docker-compose up -d

#wait for services to do work on test data (time in secs)
sleep $WAIT_FOR_SERVICES   

# Assertions
CheckService "movie-feeder" "got frame id#" 
CheckService "motion-detector" "detected contour of size" 
CheckService "object-detector" "text: person:" 
CheckService "face-detector" "detected a face" 
CheckService "face-matcher" "match found" 
CheckService "message-aggregator" "Num of faces: 1, Num of objects: 2" 
CheckService "message-aggregator" "person" 
CheckService "message-aggregator" "refrigerator" 


