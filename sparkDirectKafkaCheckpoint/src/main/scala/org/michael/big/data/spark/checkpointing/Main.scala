package org.michael.big.data.spark.checkpointing

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main extends App {

  // KafkaInput
  // Kafka Params
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CheckpointGroupId1337",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> "false"
  )

  // KafkaOutput
  val stringSerializerName: String = classOf[StringSerializer].getName

  // KafkaOutput
  val kafkaProducerProps: Properties = new Properties()
  kafkaProducerProps.put("bootstrap.servers", "localhost:9092")
  kafkaProducerProps.put("key.serializer", stringSerializerName)
  kafkaProducerProps.put("value.serializer", stringSerializerName)
  kafkaProducerProps.put("acks", "1")
  kafkaProducerProps.put("compression.type", "none")
  println(kafkaProducerProps)

  // Configuration
  val topicString = "myInputTopic"

  // Main
  val topics: Array[String] = Array(topicString)

  // Main
  // create Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("NumRecordException")
    .master("local[*]")
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.streaming.kafka.maxRatePerPartition", 10000)
    .config("spark.streaming.backpressure.pid.minRate", 100)
    .getOrCreate()

  // KafkaOutput
  // function von Iterator[ConsumerRecord[String, String]] => Unit
  def sendAsyncToKafka(iterator: Iterator[ConsumerRecord[String, String]]): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerProps)
    iterator.foreach(record => {
        producer.send(new ProducerRecord[String, String]("myOutputTopic", record.key, record.value), new ProducerCallback)
        //println(record)
    })
    producer.flush()
    producer.close()
  }

  // KafkaInput
  // create Kafka stream
  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(5L))

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
    .createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

  // commit offsets to Kafka
  stream.foreachRDD(rdd => {
    if(!rdd.isEmpty()) {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // process data
      rdd.foreachPartition(sendAsyncToKafka)

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    } else {
      println("No RDD received. Nothing to process.")
    }
  })

  // Main
  // start streaming context and wait for termination
  ssc.start()
  ssc.awaitTermination()


  // eigene Klasse mit selfType! zum Trait KafkaOutput
  // KafkaOutput
  // https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/Callback.html
  // exception - The exception thrown during processing of this record.
  //
  // Null if no error occurred. Possible thrown exceptions include:
  // Non-Retriable exceptions (fatal, the message will never be sent):
  // InvalidTopicException OffsetMetadataTooLargeException RecordBatchTooLargeException RecordTooLargeException UnknownServerException
  //
  // Retriable exceptions (transient, may be covered by increasing #.retries):
  // CorruptRecordException InvalidMetadataException NotEnoughReplicasAfterAppendException NotEnoughReplicasException OffsetOutOfRangeException TimeoutException UnknownTopicOrPartitionException
  private class ProducerCallback extends Callback {
    @Override
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if(exception != null) {
        exception.printStackTrace()
      }
    }
  }


}


