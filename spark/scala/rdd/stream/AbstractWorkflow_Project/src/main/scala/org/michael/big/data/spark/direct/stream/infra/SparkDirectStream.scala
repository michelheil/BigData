package org.michael.big.data.spark.direct.stream.infra

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.michael.big.data.spark.direct.stream.conf.ConfLoader

abstract class SparkDirectStream extends App {

  this: ConfLoader =>

  type streamInput

  // create Spark Session
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(conf.getString("spark.app.name"))
    .master(conf.getString("spark.app.master"))
    .getOrCreate()

  // create Streaming Context
  lazy val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(conf.getString("spark.batch.duration").toLong))

  // create Input Stream
  val stream: InputDStream[streamInput]

  // process DStream
  val processRDDPartition: Iterator[streamInput] => Unit

}
