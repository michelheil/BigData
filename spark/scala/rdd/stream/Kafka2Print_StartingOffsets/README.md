# Goal
In this module I play around with the offsetRanges of a simple Direct Spark Stream reading from Kafka

### Main Resource
https://spark.apache.org/docs/2.3.2/streaming-kafka-0-10-integration.html

## Start Kafka and Zookeeper
```shell script
cd /home/michael/kafka/current
bash bin/zookeeper-server-start.sh config/zookeeper.properties
bash bin/kafka-server-start.sh config/server.properties
```

## Create Kafka Topic
```shell script
./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic test
```

## Describe Kafka Topic (to check if existing)
```shell script
./kafka/current/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic test
```

## Produce messages into Topic
```shell script
./kafka/current/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test --property "parse.key=true" --property "parse.value=true" --property "key.separator=:::"
>hello:::world
>hello2:::world2
>hello3:::world3
>hello4:::world4
```

# Obeservation
Let's say I only want to read from partition 0, offset 1.

```scala
val startingOffset = new mutable.HashMap[TopicPartition, scala.Long]()
startingOffset.put(new TopicPartition(topicString, 0), 1L)
```

Before using a TopicPartition in the `Subscribe` the stream consumed this data:
```shell script
test 1 0 0
test 0 0 2
test 2 0 2
```

After using a TopicPartition in `Subscribe` the stream consumed this data:
```shell script
test 1 0 0
test 0 1 2
test 2 0 2
```

