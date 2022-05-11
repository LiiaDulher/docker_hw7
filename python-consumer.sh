#!/bin/bash

docker build -t read_tweets_image -f Dockerfile2 .

docker run -it --name python-consumer --network dulher-kafka-network -v "$(pwd)"/tweets:/opt/app/tweets --rm read_tweets_image
# docker run -it --name python-consumer --network dulher-kafka-network --rm read_tweets_image
