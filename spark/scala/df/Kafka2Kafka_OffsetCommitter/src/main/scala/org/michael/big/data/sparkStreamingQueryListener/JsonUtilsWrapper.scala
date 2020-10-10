package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition

/**
 * Hack to access private API
 * Put this class into org.apache.spark.sql.kafka010 package
 * https://stackoverflow.com/questions/46153105/how-to-get-kafka-offsets-for-structured-query-for-manual-and-reliable-offset-man
 */
object JsonUtilsWrapper {
  def offsetsToJson(partitionOffsets: Map[TopicPartition, Long]): String = {
    JsonUtils.partitionOffsets(partitionOffsets)
  }

  def jsonToOffsets(str: String): Map[TopicPartition, Long] = {
    JsonUtils.partitionOffsets(str)
  }
}