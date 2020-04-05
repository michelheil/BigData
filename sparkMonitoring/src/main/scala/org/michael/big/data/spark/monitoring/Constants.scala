package org.michael.big.data.spark.monitoring

object Constants {

  object Spark {
    lazy val SPARK_STREAMING_BACKPRESSURE_ENABLED_CONFIG = "spark.streaming.backpressure.enabled"
    lazy val SPARK_STREAMING_KAFKA_MAXRATEPERPARTITION_CONFIG = "spark.streaming.kafka.maxRatePerPartition"
    lazy val SPARK_STREAMING_BACKPRESSURE_PID_MINRATE = "spark.streaming.backpressure.pid.minRate"
  }

  object Kafka {
    lazy val KAFKA_BOOTSTRAP_SERVERS_CONFIG = "kafka.bootstrap.servers"
    lazy val AUTO_OFFSET_SERVERS_CONFIG = "auto.offset.reset"
  }
}
