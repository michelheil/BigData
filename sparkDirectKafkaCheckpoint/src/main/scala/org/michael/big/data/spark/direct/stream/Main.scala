package org.michael.big.data.spark.direct.stream

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.michael.big.data.spark.direct.stream.app.ApplicationProcessor
import org.michael.big.data.spark.direct.stream.conf.ConfLoader
import org.michael.big.data.spark.direct.stream.infra.{KafkaInput, KafkaOutput, SparkDirectStream}

object Main extends SparkDirectStream
  with ApplicationProcessor
  with KafkaInput
  with KafkaOutput
  with ConfLoader {

  // load configuration
  override val conf = loadConfigFromPath(getClass.getResource("/").getPath)

  // create overall processor for RDD stream
  override val processRDD = appProcessRDD _

  // add application specific Spark configurations
  spark.conf.set("spark.streaming.backpressure.enabled", conf.getString("spark.streaming.backpressure.enabled"))
  spark.conf.set("spark.streaming.kafka.maxRatePerPartition", conf.getString("spark.streaming.kafka.maxRatePerPartition"))
  spark.conf.set("spark.streaming.backpressure.pid.minRate", conf.getString("spark.streaming.backpressure.pid.minRate"))

  // create Spark stream reading from Kafka topic
  override val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(conf.getString("spark.batch.duration").toLong))

  override val stream: InputDStream[streamInput] = KafkaUtils
    .createDirectStream[KafkaInKey, KafkaInValue](
      ssc,
      PreferConsistent,
      Subscribe[KafkaInKey, KafkaInValue](inputTopic, kafkaParams))

  // commit offsets to Kafka
  stream.foreachRDD(rdd => {
    if(!rdd.isEmpty()) {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // process data
      rdd.foreachPartition(processRDD)

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    } else {
      println("No RDD received. Nothing to process.")
    }
  })

  // start streaming context and wait for termination
  ssc.start()
  ssc.awaitTermination()
}


