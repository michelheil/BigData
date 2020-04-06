package org.michael.big.data.spark.monitoring

import org.apache.spark.sql.SparkSession

object MainBootstrap extends App
  with ConfigLoader {

    // ensure that the folder where the conf files are located are marked as project resources
    val config = loadConfigFromPath(getClass.getResource("/").getPath)

    println(config)
    println(config.getString("auto.offset.reset"))
    println(config.getString("spark.streaming.backpressure.enabled"))
    println(config.getObject("spark.streaming").toString)

    // create SparkStreaming Job that writes from Kafka to Kafka (use Direct API)
    val spark = SparkSession.builder()
      .master(config.getString("spark.app.master"))
      .appName(config.getString("spark.app.name"))
      .config(Constants.Spark.SPARK_STREAMING_BACKPRESSURE_ENABLED_CONFIG, config.getString(Constants.Spark.SPARK_STREAMING_BACKPRESSURE_ENABLED_CONFIG))
      .getOrCreate()

    val ds1 = spark.readStream
      .format("kafka")
      .option(Constants.Kafka.KAFKA_BOOTSTRAP_SERVERS_CONFIG, config.getString(Constants.Kafka.KAFKA_BOOTSTRAP_SERVERS_CONFIG))
      .option("subscribe", config.getString("kafka.input.topic"))
      .load()

    val ds2 = ds1.selectExpr(config.getString("kafka.output.topic"), "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream.format("kafka")
      .option("checkpointLocation", config.getString("spark.app.checkpoint.location.dir"))
      .option(Constants.Kafka.KAFKA_BOOTSTRAP_SERVERS_CONFIG, config.getString(Constants.Kafka.KAFKA_BOOTSTRAP_SERVERS_CONFIG))
      .start()
}
