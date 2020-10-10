#!/usr/bin/env bash

KAFKA_TOPIC=($(grep kafka.input.topic ../src/main/resources/kafka.conf | cut -d "=" -f2))
TEST_DATA_PATH="../../sparkTheDefinitiveGuide/data/activity-data/"

for filename in ${TEST_DATA_PATH}*.json; do
  echo "Producing data of file: ${filename}"
  kafkacat -b localhost:9092 -t ${KAFKA_TOPIC} -P -l ${filename}
done
echo "All messages loaded into topic: ${KAFKA_TOPIC}"

