package org.michael.big.data.sparkStreamingQueryListener

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ListenerTester")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // https://gist.github.com/timvw/ddb5dfd470eb06786209e218fce5e190
    val props = new Properties()
    props.put("group.id", "myGroupId")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("enable.auto.commit", "false")
    val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    val listener = CommitOffsetsOnProgressQueryListener(kafkaConsumer)
    spark.streams.addListener(listener)

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testingKafkaProducer")
      .option("failOnDataLoss", "false") // in case offsets or entire topic are getting deleted
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testingKafkaProducerOut")
      .option("checkpointLocation", "/home/michael/sparkCheckpoint/")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
