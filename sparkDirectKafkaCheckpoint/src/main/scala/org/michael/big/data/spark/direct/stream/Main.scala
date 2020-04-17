package org.michael.big.data.spark.direct.stream

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main extends SparkDirectStream
  with KafkaInput
  with KafkaOutput
  with ConfLoader {

  // load configuration
  override val conf: Config = loadConfigFromPath(getClass.getResource("/").getPath)

  // define application specific input type
  type KafkaInKey = String
  type KafkaInValue = String
  type KafkaOutKey = String
  type KafkaOutValue = String
  type streamInput = ConsumerRecord[KafkaInKey, KafkaInValue]

  // define application specific logic to process RDDs
  override val processRDD = sendAsyncToKafka _

  // add application specific Spark configurations
  spark.conf.set("spark.streaming.backpressure.enabled", conf.getString("spark.streaming.backpressure.enabled"))
  spark.conf.set("spark.streaming.kafka.maxRatePerPartition", conf.getString("spark.streaming.kafka.maxRatePerPartition"))
  spark.conf.set("spark.streaming.backpressure.pid.minRate", conf.getString("spark.streaming.backpressure.pid.minRate"))

  // create Kafka stream
  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(conf.getString("spark.batch.duration").toLong))

  val stream: InputDStream[streamInput] = KafkaUtils
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


