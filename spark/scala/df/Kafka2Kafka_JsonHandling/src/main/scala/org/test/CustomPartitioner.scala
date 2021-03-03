package org.test

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import java.util

class CustomPartitioner extends Partitioner {

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any,valueBytes: Array[Byte],cluster: Cluster): Int = {
    if (!valueBytes.isEmpty && valueBytes.map(_.toChar).mkString == "Bob") {
      println(valueBytes.map(_.toChar).mkString)
      1
    } else {
      println(valueBytes.map(_.toChar).mkString)
      0
    }
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}

