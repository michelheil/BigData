package org.michael.big.data.spark

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext

import scala.collection.mutable

/*
  Goal: Read particular offset in Spark Direct Streaming from Kafka Topic
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

  val topicString = "myTestTopicPart"
  val topics: Array[String] = Array(topicString)

  // create Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("ReadFromGivenOffset")
    .master("local[*]")
    .getOrCreate()

  /*
  This is the addition required to start reading Kafka topic from a particular partition.
  Running the stream without starting from a particular offset showed that the distribution of the data in the topic is like this
  myTestTopicPart 1 0 0
  myTestTopicPart 2 0 2
  myTestTopicPart 0 0 2

  Let's say I only want to read from partition 0, offset 1.
   */
  val startingOffset = new mutable.HashMap[TopicPartition, scala.Long]()
  startingOffset.put(new TopicPartition(topicString, 0), 1L)

  // create Kafka stream
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10L))
  val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, startingOffset))

  // print output to console
  stream.foreachRDD(rdd => {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition(it => {
      val o = offsetRanges(TaskContext.get.partitionId())
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      it.foreach(_ => {
        println(Thread.currentThread.getName)
      })
    })
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  })

  // start streaming context and wait for termination
  ssc.start()
  ssc.awaitTermination()
}
