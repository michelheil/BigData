package org.michael.big.data.spark.partitioning

import org.apache.spark.Partitioner

class KeyPartitioner extends Partitioner {
  override def numPartitions: Int = 3

  def getPartition(key: Any): Int = {
    key.asInstanceOf[Int] - 1 // -1 because 3 partitions are: 0, 1, and 2.
  }
}