import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object Main extends App {

  // Properties
  private val kafkaProps: Properties = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1")
  kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "2")
  kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")

  // Fire and Forget
  val fireProducer = new KafkaProducer[String, String](kafkaProps)

  time{
    for (ii <- 1 to 10000) {
      val record = new ProducerRecord[String, String]("testingKafkaProducer", ii.toString(), "world!")
      try {
        fireProducer.send(record)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  // Synchronous
  val syncProducer = new KafkaProducer[String, String](kafkaProps)

  time{
    for (ii <- 1 to 10000) {
      val record = new ProducerRecord[String, String]("testingKafkaProducer", ii.toString(), "world!")
      try {
        syncProducer.send(record).get()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  // Asynchronous
  val asyncProducer = new KafkaProducer[String, String](kafkaProps)

  time{
    for (ii <- 1 to 10000) {
      val record = new ProducerRecord[String, String]("testingKafkaProducer", ii.toString(), "world!")
      try {
        asyncProducer.send(record, new compareProducerCallback)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    asyncProducer.flush() // Invoking this method makes all buffered records immediately available to send
                          // (even if linger.ms is greater than 0) and blocks on the completion of the
                          // requests associated with these records.
  }

  // Callback trait only contains the one abstract method onCompletion
  private class compareProducerCallback extends Callback {
    @Override
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) {
        exception.printStackTrace()
      }
    }
  }

  // measure time of a code block
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}
