import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import java.util.Properties
import java.time.Duration

import scala.collection.JavaConverters._
import scala.io.Source

object AppErrorReload extends App {

  val propsConsumer = new Properties()
  propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup")
  propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // hard code
  propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // hard code
  propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // hard code
  propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // hard code
  propsConsumer.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1") // hard code
  val consumer = new KafkaConsumer[String, String](propsConsumer)

  val propsProducer = new Properties()
  propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  propsProducer.put(ProducerConfig.ACKS_CONFIG, "all") // hard code
  propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") // hard code
  propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") // hard code
  val producer = new KafkaProducer[String, String](propsProducer)

  case class MetaKafkaMessage(appName: String, topicName: String, partition: Int, offset: Long)

  def csvToMetaKafkaMessages(fileName: String): Iterator[MetaKafkaMessage] = {
    val triggerFile = Source
      .fromInputStream(getClass.getResourceAsStream(fileName))
      .getLines

    val messagesMetaList = triggerFile
      .map(line => line.split(",").map(_.trim))
      .map(line => MetaKafkaMessage(line(0), line(1), line(2).toInt, line(3).toLong))
    messagesMetaList
  }

  val msgMetaList = csvToMetaKafkaMessages("/app-error-reload.csv")

  msgMetaList.foreach(m => {
    val topicPartition = new TopicPartition(m.topicName, m.partition)
    consumer.assign(List(topicPartition).asJavaCollection)
    consumer.seek(topicPartition, m.offset)

    val polledRecord = consumer.poll(Duration.ofMillis(500)).asScala
    val polledFirstRecordOption = polledRecord.collectFirst {
      case cr: ConsumerRecord[String,String] => (cr.key(), cr.value())
    }

    polledFirstRecordOption match {
      case Some(cr) => {
        val replaceAppNameInKey = cr._1.replace(m.appName, m.appName + "-error")
        val producerRecord = new ProducerRecord[String, String](m.topicName, m.partition, replaceAppNameInKey, cr._2)
        val meta: RecordMetadata = producer.send(producerRecord).get()
        producer.flush()

        println(s"Message was successfully re-ingested " +
          s"into topic ${m.topicName} " +
          s"in partition ${meta.partition()} " +
          s"at offset ${meta.offset()}")
      }
      case None => throw new NoSuchElementException(
          s"The offset ${m.offset} " +
          s"in partition ${m.partition} " +
          s"for topic ${m.topicName} could not be found."
      )
    }
  })
}
