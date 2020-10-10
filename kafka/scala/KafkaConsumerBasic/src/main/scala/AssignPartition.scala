import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import collection.JavaConverters._

object AssignPartition extends App {

  val topic = "myOutputTopic"

  val props = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test3")
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
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

}
