import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import collection.JavaConverters._

object AsyncCommitWithCallback extends App {

  // define topic
  val topic = "myOutputTopic"

  // set properties
  val props = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "AsyncCommitter5")
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")

  // create KafkaConsumer and subscribe
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List(topic).asJavaCollection)

  // initialize global counter
  val atomicLong = new AtomicLong(0)
  println(s"Initialising atomicLong: $atomicLong")

  // consume message
  try {
    while(true) {
      val records: Iterable[ConsumerRecord[String, String]] = consumer.poll(Duration.ofMillis(1)).asScala
      println(s"Polling ${records.size} messages")
      if(records.nonEmpty) {
        for (data <- records) {
          print(s"Offset: ${data.offset}; ")
          print(s"Partition: ${data.partition}; ")
          print(s"Timestamp: ${data.timestamp}; ")
          print(s"Key: ${data.key}; ")
          println(s"Value: ${data.value}")
        }
        consumer.commitAsync(new KeepOrderAsyncCommit)
      }

    }

  } catch {
    case ex: KafkaException => ex.printStackTrace()
  } finally {
    consumer.commitSync()
    consumer.close()
  }


  class KeepOrderAsyncCommit extends OffsetCommitCallback {
    // keeping position of this callback instance
    val position = atomicLong.incrementAndGet()

    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      // retry
      if(exception != null){
        if(position == atomicLong.get) {
          consumer.commitAsync(this)
        }
      }
      println(s"AtomicLong: $position")
      println(s"Committing: $offsets")
    }
  }

}
