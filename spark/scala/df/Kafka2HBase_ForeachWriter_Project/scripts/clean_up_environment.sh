#!/usr/bin/env bash

cd /home/michael/kafka/current
echo "Kafka stopping ..."
bash bin/kafka-server-stop.sh &
echo "Kafka stopped"
sleep 5
echo "Zookeeper stopping ..."
bash bin/zookeeper-server-stop.sh &
echo "Zookeeper stopped"



