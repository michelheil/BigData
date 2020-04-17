package org.michael.big.data.spark.direct.stream

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

trait KafkaOutput {

  // typesafe configuration Config has to be provided
  this: ConfLoader =>

  type KafkaOutKey
  type KafkaOutValue

  val stringSerializerName: String = classOf[StringSerializer].getName

  lazy val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", conf.getString("bootstrap.servers"))
    props.put("key.serializer", stringSerializerName) // ToDo: define serializer based on abstract type
    props.put("value.serializer", stringSerializerName) // ToDo: define serializer based on abstract type
    props.put("acks", conf.getString("acks"))
    props.put("compression.type", conf.getString("compression.type"))
    props
  }

  // Asynchronous Producer of KafkaInput data
  def sendAsyncToKafka(iterator: Iterator[ConsumerRecord[KafkaOutKey, KafkaOutValue]]): Unit = {
    val producer = new KafkaProducer[KafkaOutKey, KafkaOutValue](kafkaProducerProps)
    iterator.foreach(record => {
      println(record)
      producer.send(new ProducerRecord[KafkaOutKey, KafkaOutValue](conf.getString("output.topic"), record.key, record.value), new ProducerCallback)
    })
    producer.flush()
    producer.close()
    //iterator.map(_ => ())
  }

}
