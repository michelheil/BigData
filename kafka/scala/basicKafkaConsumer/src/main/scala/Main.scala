import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import collection.JavaConverters._

object Main extends App {

    val topic = "myInputTopic2"

    val props = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    val topicPartition: TopicPartition = new TopicPartition(topic, 0)
    consumer.assign(java.util.Collections.singletonList(topicPartition))
    val offset: Long = consumer.position(topicPartition) - 1
    consumer.seek(topicPartition, offset)
    val record: Iterable[ConsumerRecord[String, String]] = consumer.poll(Duration.ofMillis(500)).asScala
    for (data <- record) {
          val value: String = data.value()
        println(value)
    }

    /*
          val partitionList:ArrayList[TopicPartition] = new ArrayList[TopicPartition]()
      val topicPartition1 = new TopicPartition("topicr1p3", 0)
      val topicPartition2 = new TopicPartition("othertopicr1p3", 2)
      partitionList.add(topicPartition1)
      partitionList.add(topicPartition2)
      consumer.assign(partitionList)
     */

}
