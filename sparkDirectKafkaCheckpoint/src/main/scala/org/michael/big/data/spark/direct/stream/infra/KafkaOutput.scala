package org.michael.big.data.spark.direct.stream.infra

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.michael.big.data.spark.direct.stream.conf.ConfLoader

trait KafkaOutput {

  // typesafe configuration Config has to be provided
  this: ConfLoader =>

  type KafkaOutKey
  type KafkaOutValue

  val stringSerializerName: String = classOf[StringSerializer].getName
  val longSerializerName: String = classOf[LongSerializer].getName

  lazy val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", conf.getString("bootstrap.servers"))
    props.put("key.serializer", longSerializerName) // ToDo: define serializer based on abstract type
    props.put("value.serializer", stringSerializerName) // ToDo: define serializer based on abstract type
    props.put("acks", conf.getString("acks"))
    props.put("compression.type", conf.getString("compression.type"))
    props
  }

  // create producer
  lazy val producer = new KafkaProducer[KafkaOutKey, KafkaOutValue](kafkaProducerProps)
}
