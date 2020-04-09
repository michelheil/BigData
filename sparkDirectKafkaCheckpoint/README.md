# Goal
In this module I play around with Spark's Checkpointing trying to reproduce Exception
```shell script
java.lang.IllegalArgumentException: requirement failed: numRecords must not be negative
```
## Learnings:
- use Spark's updateStateByKey
- apply Spark's checkpointing and create SparkSession based on checkpoint directory
- manipulate Offsets of a Kafka Consumer Group through shell tool
- creating Scala Option
- "<method> _" converts a def into a scala.function

## Idea
Create a Spark Streaming (Direct API) reading from Kafka with Spark's checkpointing enabled.
- Let stream job consume at least one message and commit consumed messages back to Kafka
- Validate `__consumer_offset` topic to see the commitment
- Validate `checkpoint file` to see commitment
- Use kafka-topics.sh to reset offset and therefore undo latest commit
- Try to consume another message and see what happens.

# Background
## Spark Kafka Integration Guide Documentation
https://spark.apache.org/docs/2.3.2/streaming-kafka-0-10-integration.html#checkpoints

### Checkpoint
If you enable Spark checkpointing, offsets will be stored in the checkpoint. 
This is easy to enable, but there are drawbacks. Your output operation must be 
idempotent, since you will get repeated outputs; transactions are not an option. 
Furthermore, you cannot recover from a checkpoint if your application code has
changed. For planned upgrades, you can mitigate this by running the new code at 
the same time as the old code (since outputs need to be idempotent anyway, 
they should not clash). But for unplanned failures that require code changes, 
you will lose data unless you have another way to identify known good starting offsets.

### Commit Kafka offsets
Kafka has an offset commit API that stores offsets in a special Kafka topic. 
By default, the new consumer will periodically auto-commit offsets. 
This is almost certainly not what you want, because messages successfully 
polled by the consumer may not yet have resulted in a Spark output operation, 
resulting in undefined semantics. This is why the stream example above sets 
“enable.auto.commit” to false. However, you can commit offsets to Kafka after 
you know your output has been stored, using the commitAsync API. The benefit 
as compared to checkpoints is that Kafka is a durable store **regardless of 
changes to your application code**. However, Kafka is not transactional, 
so your outputs must still be idempotent.

```scala
stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

  // some time later, after outputs have completed
  stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
}
```
As with HasOffsetRanges, the cast to CanCommitOffsets will only succeed if 
called on the result of createDirectStream, not after transformations. 
The commitAsync call is threadsafe, but must occur after outputs if 
you want meaningful semantics.


## Spark Streaming Documentation
https://spark.apache.org/docs/2.3.2/streaming-programming-guide.html#checkpointing

### How to configure checkpointing
```scala
// Function to create and setup a new StreamingContext
def functionToCreateContext(): StreamingContext = {
  val ssc = new StreamingContext(...)   // new context
  val lines = ssc.socketTextStream(...) // create DStreams
  ...
  ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
  ssc
}

// Get StreamingContext from checkpoint data or create a new one
val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

// Do additional setup on context that needs to be done,
// irrespective of whether it is being started or restarted
context. ...

// Start the context
context.start()
context.awaitTermination()
```

# Local Setup on my machine

## Start Kafka and Zookeeper
```shell script
bash /home/michael/kafka/current/bin/zookeeper-server-start.sh /home/michael/kafka/current/config/zookeeper.properties
bash /home/michael/kafka/current/bin/kafka-server-start.sh /home/michael/kafka/current/config/server.properties
```

## Create Kafka Topic
```shell script
./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic myTestTopicCheck
```

## Describe Kafka Topic (to check if existing)
```shell script
./kafka/current/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic myTestTopicCheck
```

## Produce messages into Topic
```shell script
./kafka/current/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic myTestTopicCheck --property "parse.key=true" --property "parse.value=true" --property "key.separator=:::"
>hello:::world
>hello2:::world2
>hello3:::world3
>hello4:::world4
```

# Changing Offset for Consumer Group
```shell script
./kafka/current/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "CheckpointGroupId1337"

./kafka/current/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group "CheckpointGroupId1337" --reset-offsets --topic "myTestTopicCheck:0" --shift-by -5 --execute
```


# Obeservation
- Unfortunately, it is not clear how checkpoint files are storing the offset
