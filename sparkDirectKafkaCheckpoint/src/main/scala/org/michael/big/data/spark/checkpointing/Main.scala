package org.michael.big.data.spark.checkpointing

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main extends App {

  // Kafka Params
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CheckpointGroupId1337",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false"
  )

  val topicString = "myTestTopicCheck"
  val topics: Array[String] = Array(topicString)

  // create Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("CheckpointNumRecordException")
    .master("local[*]")
    .getOrCreate()

  // create Kafka stream
  def functionToCreateContext(): StreamingContext = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10L))   // new context

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
/*
    val keyValueDStream: DStream[(String, String)] = stream.map(cr => (cr.key(), cr.value()))
    val keyUpdateDStream: DStream[(String, String)] = keyValueDStream.updateStateByKey(updateFunction _)

    keyUpdateDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it =>
        it.foreach(println))
    })
*/
    // commit offsets to Kafka
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(it =>
        it.foreach(println))
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.checkpoint("/home/michael/sparkCheckpoint")   // set checkpoint directory
    ssc
  }
  val context = StreamingContext.getOrCreate("/home/michael/sparkCheckpoint", functionToCreateContext _)

  // start streaming context and wait for termination
  context.start()
  context.awaitTermination()

  // update function that is used in the updateStateByKey
  // values: contains the values of a key in the current batch (may be empty)
  // state: is an optional state object. It might be missing if there was no previous state for the key
  def updateFunction(values: Seq[String], state: Option[String]): Option[String] = {
    state match {
      case Some(x) if !values.isEmpty & !values.contains(x) => Some(values(values.size-1))
      case Some(x) => Some(x)
      case None => None
    }
  }

}
