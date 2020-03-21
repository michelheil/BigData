package org.michael.big.data.sparkStreamingQueryListener

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.spark.sql.kafka010.JsonUtilsWrapper
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent

case class CommitOffsetsOnProgressQueryListener(kafkaConsumer: KafkaConsumer[_, _]) extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = { }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {

    val offsets = event
      .progress
      .sources // assumption: sources are all kafka, all on the same cluster
      .map(_.endOffset)
      .flatMap(JsonUtilsWrapper.jsonToOffsets) // extract all offsets
      .groupBy(_._1) // take the smallest offset per topic partition
      .mapValues(_.sortWith((a, b) => a._2.compareTo(b._2) < 0).head._2)
      .mapValues(new OffsetAndMetadata(_))

    import scala.collection.JavaConversions._
    kafkaConsumer.commitSync(offsets)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = { }
}