package org.michael.big.data.spark.monitoring

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object MainBootstrap extends App
  with ConfLoader {

  // ensure that the folder where the conf files are located are marked as project resources
  val conf = loadConfigFromPath(getClass.getResource("/").getPath)

  // create SparkStreaming Job that writes from Kafka to Kafka (use Direct API)
  val spark = SparkSession.builder()
    .master(conf.getString("spark.app.master"))
    .appName(conf.getString("spark.app.name"))
    .config(Const.SparkStreamConf.BACKPRESSURE_ENABLED, conf.getString(Const.SparkStreamConf.BACKPRESSURE_ENABLED))
    .config(Const.SparkStreamConf.KAFKA_MAXRATEPERPARTITION, conf.getString(Const.SparkStreamConf.KAFKA_MAXRATEPERPARTITION))
    .config(Const.SparkStreamConf.BACKPRESSURE_PID_MINRATE, conf.getString(Const.SparkStreamConf.BACKPRESSURE_PID_MINRATE))
    .getOrCreate()

  val ds1 = spark.readStream
    .format("kafka")
    .option(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS, conf.getString(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS))
    .option("subscribe", conf.getString("kafka.input.topic"))
    .load()

  val ds2 = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .format("kafka")
    .option("checkpointLocation", conf.getString("spark.app.checkpoint.location.dir"))
    .option(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS, conf.getString(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS))
    .option("topic", conf.getString("kafka.output.topic"))
    .start()

  spark.streams.awaitAnyTermination()
}
