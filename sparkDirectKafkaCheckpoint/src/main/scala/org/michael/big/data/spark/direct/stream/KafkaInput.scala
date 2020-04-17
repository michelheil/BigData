package org.michael.big.data.spark.direct.stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.immutable.HashMap

trait KafkaInput {

  // typesafe configuration Config has to be provided
  this: ConfLoader =>

  lazy val inputTopic: Array[String] = Array(conf.getString("input.topic"))

  lazy val kafkaParams: Map[String, Object] = HashMap[String, Object](
    "bootstrap.servers" -> conf.getString("bootstrap.servers"),
    "group.id" -> conf.getString("group.id"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> conf.getString("auto.offset.reset"),
    "enable.auto.commit" -> conf.getString("enable.auto.commit")
  )

}
