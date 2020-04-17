package org.michael.big.data.spark.direct.stream

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

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
class ProducerCallback extends Callback {

  @Override
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if(exception != null) {
      exception.printStackTrace()
    }
  }
}