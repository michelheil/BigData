package org.michael.big.data.sparkStreamingQueryListener

import org.apache.spark.sql.SparkSession

object ListenerBootstrap {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ListenerTester")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testingKafkaProducer")
      .option("failOnDataLoss", "false") // in case offsets or entire topic are getting deleted
      .load()

/*
    import spark.implicits._
    val dfOut = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

*/
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
