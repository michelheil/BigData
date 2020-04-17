package org.michael.big.data.spark.direct.stream

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

trait KafkaOutput {

  // typesafe configuration Config has to be provided
  this: ConfLoader =>

  val stringSerializerName: String = classOf[StringSerializer].getName

  lazy val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", conf.getString("bootstrap.servers"))
    props.put("key.serializer", stringSerializerName)
    props.put("value.serializer", stringSerializerName)
    props.put("acks", conf.getString("acks"))
    props.put("compression.type", conf.getString("compression.type"))
    props
  }

  // Asynchronous Producer of KafkaInput data
  def sendAsyncToKafka(iterator: Iterator[ConsumerRecord[String, String]]): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerProps)
    iterator.foreach(record => {
      println(record)
      producer.send(new ProducerRecord[String, String](conf.getString("output.topic"), record.key, record.value), new ProducerCallback)
    })
    producer.flush()
    producer.close()
  }

}
