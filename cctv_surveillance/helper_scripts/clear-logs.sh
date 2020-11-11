#!/bin/bash

truncate -s 0 $(sudo docker inspect --format='{{.LogPath}}' movie-feeder)
truncate -s 0 $(sudo docker inspect --format='{{.LogPath}}' face-detector)
truncate -s 0 $(sudo docker inspect --format='{{.LogPath}}' message-aggregator)
truncate -s 0 $(sudo docker inspect --format='{{.LogPath}}' face-matcher)
truncate -s 0 $(sudo docker inspect --format='{{.LogPath}}' motion-detector)
truncate -s 0 $(sudo docker inspect --format='{{.LogPath}}' object-detector)
