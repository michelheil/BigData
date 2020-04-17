package org.michael.big.data.spark.direct.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

abstract class SparkDirectStream extends App {

  this: ConfLoader =>

  type streamInput

  // create Spark Session
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(conf.getString("spark.app.name"))
    .master(conf.getString("spark.app.master"))
    .getOrCreate()

  // create Streaming Context
  val ssc: StreamingContext

  // create Input Stream
  val stream: InputDStream[streamInput]

  // process DStream
  val processRDD: Iterator[streamInput] => Unit

}
