package org.michael.big.data.spark.monitoring

object Const {

  object SparkStreamConf {
    lazy val BACKPRESSURE_ENABLED = "spark.streaming.backpressure.enabled"
    lazy val KAFKA_MAXRATEPERPARTITION = "spark.streaming.kafka.maxRatePerPartition"
    lazy val BACKPRESSURE_PID_MINRATE = "spark.streaming.backpressure.pid.minRate"
  }

  object KafkaConf {
    lazy val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
    lazy val AUTO_OFFSET_SERVERS = "auto.offset.reset"
  }
}
