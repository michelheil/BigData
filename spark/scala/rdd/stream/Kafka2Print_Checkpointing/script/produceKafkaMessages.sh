#!/usr/bin/env bash

KAFKA_TOPIC="test"
TEST_DATA_FILE="/home/michael/GitHubRepositories/BigData/spark/scala/rdd/stream/Kafka2Print_Checkpointing/src/test/resources/1000.json"

iterator=1

while [ $iterator -le 5 ]
do
  kafkacat -b localhost:9092 -t ${KAFKA_TOPIC} -P -l ${TEST_DATA_FILE}
  echo $iterator
  ((iterator++))
done
