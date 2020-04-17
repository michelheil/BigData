package org.michael.big.data.spark.direct.stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

abstract class SparkDirectStream[T<: ConsumerRecord[String, String]] extends App {

  this: ConfLoader =>

  // create Spark Session
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(conf.getString("spark.app.name"))
    .master(conf.getString("spark.app.master"))
    .getOrCreate()

  // create Streaming Context
  val ssc: StreamingContext

  // create Input Stream
  val stream: InputDStream[T]

  // process DStream
  val processRDD: Iterator[T] => Unit

}
