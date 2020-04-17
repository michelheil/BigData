package org.michael.big.data.spark.direct.stream

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.parsing.json.JSON

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
      val parsedValue: Option[Any] = JSON.parseFull(record.value.toString)
      val valueAsJSON: Map[String, Any] = parsedValue match {
        case Some(map: Map [String, Any]) => map
        case _ => Map[String, Any]()
      }

      /*
      import org.json4s._
import org.json4s.native.JsonMethods._

implicit val formats = DefaultFormats

case class ParsedPage(crawlDate: String, domain:String, url:String, text: String)

val js = """ {
"crawlDate": "20150226",
"domain": "0x20.be",
"url": "http://0x20.be/smw/index.php?title=99_Bottles_of_Beer&oldid=6214",
"text": "99 Bottles of Beer From Whitespace (Hackerspace Gent) Revision as of 14:43, 8 August 2012 by Hans ( Talk | contribs ) 99 Bottles of Beer Where: Loading map... Just me, with 99 bottles of beer and some friends. Subpages"
}"""


parse(js).extract[ParsedPage]


       */

      println(valueAsJSON("Arrival_Time"))
      println(valueAsJSON("Device"))
      println(valueAsJSON("Index"))
      println(valueAsJSON("Model"))

      producer.send(new ProducerRecord[KafkaOutKey, KafkaOutValue](conf.getString("output.topic"), record.key, record.value), new ProducerCallback)
    })
    producer.flush()
    producer.close()
    //iterator.map(_ => ())
  }

}
