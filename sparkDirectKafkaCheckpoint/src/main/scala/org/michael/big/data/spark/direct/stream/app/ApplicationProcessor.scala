package org.michael.big.data.spark.direct.stream.app

import java.io.File

import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import org.michael.big.data.spark.direct.stream.conf.ConfLoader
import org.michael.big.data.spark.direct.stream.infra.{KafkaOutput, ProducerCallback}

trait ApplicationProcessor extends KafkaOutput with ConfLoader {

  // define application specific input type
  type KafkaInKey = String
  type KafkaInValue = String
  type KafkaOutKey = Long
  type KafkaOutValue = String
  type streamInput = ConsumerRecord[KafkaInKey, KafkaInValue]

  val schemaAvro = new Schema.Parser().parse(new File("src/main/resources/person.avsc"))

  // define application specific logic to process RDDs
  implicit val formats = DefaultFormats

  // Asynchronous Producer of KafkaInput data
  def appProcessRDDPartition(iterator: Iterator[ConsumerRecord[KafkaInKey, KafkaInValue]]): Unit = {
    iterator.foreach(record => {
      println(s"""Processing record ${record}""")
      val js: KafkaInValue = record.value()
      val parsedValue: InputData = parse(js.asInstanceOf[String]).extract[InputData]
      producer.send(new ProducerRecord[KafkaOutKey, KafkaOutValue](outputTopic, parsedValue.Arrival_Time, record.value), new ProducerCallback)
    })
    producer.flush()
  }
}
