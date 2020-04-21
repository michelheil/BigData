package org.michael.big.data.spark.direct.stream.infra

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.michael.big.data.spark.direct.stream.conf.ConfLoader

import scala.collection.immutable.HashMap

trait KafkaInput {

  // typesafe configuration Config has to be provided
  this: ConfLoader =>

  type KafkaInKey
  type KafkaInValue

  lazy val inputTopic: Array[String] = Array(conf.getString("app.input.topic"))

  lazy val kafkaParams: Map[String, Object] = HashMap[String, Object](
    "bootstrap.servers" -> conf.getString("bootstrap.servers"),
    "group.id" -> conf.getString("group.id"),
    "key.deserializer" -> classOf[StringDeserializer], // ToDo: define deserializer based on abstract type
    "value.deserializer" -> classOf[StringDeserializer], // ToDo: define deserializer based on abstract type
    "auto.offset.reset" -> conf.getString("auto.offset.reset"),
    "enable.auto.commit" -> conf.getString("enable.auto.commit")
  )

  def createKafkaInputStream[K, V](ssc: StreamingContext): InputDStream[ConsumerRecord[K, V]] = {
    KafkaUtils.createDirectStream[K, V](
        ssc,
        PreferConsistent,
        Subscribe[K, V](inputTopic, kafkaParams))
  }

}
