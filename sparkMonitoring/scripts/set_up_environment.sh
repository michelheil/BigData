#!/usr/bin/env bash

# defining parameters
KAFKA_INPUT_TOPIC=($(grep kafka.input.topic ../src/main/resources/kafka.conf | cut -d "=" -f2))
KAFKA_OUTPUT_TOPIC=($(grep kafka.output.topic ../src/main/resources/kafka.conf | cut -d "=" -f2))

# starting Zookeeper and Kafka
cd /home/michael/kafka/current
echo "Zookeeper starting ..."
bash bin/zookeeper-server-start.sh config/zookeeper.properties & > /dev/null 2>&1
echo "Zookeeper started"
sleep 10
echo "Kafka starting ..."
bash bin/kafka-server-start.sh config/server.properties & > /dev/null 2>&1
echo "Kafka started"

# creating Kafka topics
cd /home/michael
echo "Creating topic ${KAFKA_INPUT_TOPIC}"
bash ./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ${KAFKA_INPUT_TOPIC}
echo "Creating topic ${KAFKA_OUTPUT_TOPIC}"
bash ./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ${KAFKA_OUTPUT_TOPIC}


