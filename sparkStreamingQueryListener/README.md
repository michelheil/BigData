In this module, I am trying to play around with Spark's StreamingQueryListener.

As a use case I plan to manually commit offset of a Kafka Topic within a Structured Stream of Spark.


Idea:
* Start Kafka on localhost (incl. Zookeeper)
* Create Topic with one partition
* Run Spark Structured Streaming subscribing to this topic
* Observe internal topic __consumer_offset and figure out what the group.id is and if offsets are getting committed
(based on documentation it is not possible to set group.id in Structured Streaming and also not possible
to enable auto.offset.reset as Kafka properties.)
* Create a StreamingQueryListener that manually commits offsets to Kafka during `onQueryProcess`
* Observe internal topic __consumer_offset and figure out what the group.id is and if offsets are getting committed

Instructions:

start Zookeeper

start Kafka

create Kafka topics

```shell script
./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testingKafkaProducer
./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testingKafkaProducerOut
```

sbt package

```shell script
cd /home/michael/spark/spark-2.4.0-bin-hadoop2.7
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --master local[1] --class org.michael.big.data.sparkStreamingQueryListener.Main /home/michael/GitHubRepositories/BigData/sparkStreamingQueryListener/target/scala-2.11/sparkstreamingquerylistener_2.11-0.1.jar
```

# Approach
Deploy and run a Spark Structured Streaming application that reads from a Kafka topic and writes to another Kafka topic. 

# Code snippet
```scala
    // create SparkSession
    val spark = SparkSession.builder()
      .appName("ListenerTester")
      .master("local[1]")
      .getOrCreate()

    // read from Kafka topic
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testingKafkaProducer")
      .option("failOnDataLoss", "false")
      .load()

    // write to Kafka topic and set checkpoint directory for this stream
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testingKafkaProducerOut")
      .option("checkpointLocation", "/home/.../sparkCheckpoint/")
      .start()
```

# Important Notes of Spark Streaming + Kafka Integration Guide
https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
* **group.id:** Kafka source will create a unique group id for each query automatically.
(https://github.com/apache/spark/blob/v2.4.0/external/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaSourceProvider.scala#L124)
* **auto.offset.reset:** Set the source option startingOffsets to specify where to start instead. 
*Structured Streaming manages which offsets are consumed internally, rather than rely on the kafka Consumer to do it.*
* **enable.auto.commit:** Kafka source doesn’t commit any offset.

In summary:
* Structured Streaming is currently not possible to define your custom group.id for Kafka Consumer.
* Structured Streaming is managing the offsets internally and not committing back to Kafka (also not automatically).

# Observation
## Checkpoint by Spark 
The corresponding offset can be found in the checkpoint directory:

myCheckpointDir/offsets/
```shell script
{"testingKafkaProducer":{"0":1}}
```
Here the entry in the checkpoint file confirms that the next offset to be consumed is `1`.
It implies that the application already was reading offset `0` from partition `0` of the topic named `testingKafkaProducer`.

More on the fault-tolerance-semantics are given in the Spark Documentation:
https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics

## Offset Management by Kafka
However, as stated in the documentation, the offset is **not** committed back to Kafka. 
This can be checked by executing the `kafka-consumer-groups.sh` of the Kafka installation.
> ./kafka/current/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "spark-kafka-source-92ea6f85-[...]-driver-0"
```shell script
TOPIC                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID      HOST         CLIENT-ID
testingKafkaProducer 0          -               1               -    consumer-1-[...] /127.0.0.1   consumer-1
```
The current offset for this application is unknown to *Kafka* as it has never been committed.

## Using Spark "StreamingQueryListener"
A separate KafkaConsumer is initialised and the offsets are committed during the callback function `onQueryProgress`.
However, the offset are only committed based on the given group.id of the separate KafkaConsumer.

```shell script
./kafka/current/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "myGroupId"
Consumer group 'myGroupId' has no active members.

TOPIC                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
testingKafkaProducer 0          2               2               0               -               -               -
```

# Discussion on Spark community about Kafka committing
https://issues.apache.org/jira/browse/SPARK-27549
https://github.com/apache/spark/pull/24613
https://github.com/HeartSaVioR/spark-sql-kafka-offset-committer

