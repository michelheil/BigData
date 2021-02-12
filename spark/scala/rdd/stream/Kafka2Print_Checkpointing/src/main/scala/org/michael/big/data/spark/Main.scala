package org.michael.big.data.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

/*
  Goal: Recover from Checkpoint after failure or forces re-start
 */
object Main extends App {

  // Kafka Params
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "myGroupId1337",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false"
  )

  val checkpointDir = "/tmp/checkpoint/sparkDirectKafkaCheckpointing"
  val checkpointDuration: Long = 5000L

  val schema = new StructType()
    .add("Arrival_Time", LongType)
    .add("Creation_Time", LongType)
    .add("Device", StringType)
    .add("Index", IntegerType)
    .add("Model", StringType)
    .add("User", StringType)
    .add("gt", StringType)
    .add("x", DoubleType)
    .add("y", DoubleType)
    .add("z", DoubleType)

  val topicString = "test"
  val topics: Array[String] = Array(topicString)

  // create Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("RecoverFromCheckpoint")
    .master("local[*]")
    .getOrCreate()

  // create Kafka stream
  // https://spark.apache.org/docs/latest/streaming-programming-guide.html#how-to-configure-checkpointing
  // http://spark.apache.org/docs/2.3.2/streaming-kafka-0-10-integration.html#checkpoints
  // --> "Your output operation must be idempotent, since you will get repeated outputs;"
  def setupContext(ss: SparkSession, dir: String): StreamingContext = {
    val ssc = new StreamingContext(ss.sparkContext, Seconds(10L))

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    //stream.checkpoint(Duration(5000L)) // cannot be used, because this method will persist the data which will lead to Exception "Kafka ConsumerRecord is not serializable."

    // print output to console
    stream.foreachRDD(rdd => {
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val kafkaValueRdd: RDD[String] = rdd.mapPartitions(Processor.extractValue)
      val df: DataFrame = spark.read.schema(schema).json(kafkaValueRdd)
      println(df.count())
//        .groupBy(col("Device"), col("Model"))
//        .agg(max("y").as("maxY"))
//        .orderBy(col("maxY").desc)
//        .show(3)
      //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    // set checkpoint directory and start streaming context and wait for termination
    ssc.checkpoint(dir)
    ssc
  }

  val streamingContext = StreamingContext
    .getOrCreate(
      checkpointDir,
      () => setupContext(spark, checkpointDir)
    )


  streamingContext.start()
  streamingContext.awaitTermination()
}