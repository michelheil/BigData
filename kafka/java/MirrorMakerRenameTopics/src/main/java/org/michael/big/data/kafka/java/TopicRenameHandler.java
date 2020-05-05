package org.michael.big.data.kafka.java;

import java.util.Collections;
import java.util.List;
import kafka.consumer.BaseConsumerRecord;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * An example implementation of MirrorMakerMessageHandler that allows to rename topic.
 */
public class TopicRenameHandler implements MirrorMaker.MirrorMakerMessageHandler {
  private final String newName;

  public TopicRenameHandler(String newName) {
    this.newName = newName;
  }

  public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
    return Collections.singletonList(new ProducerRecord<byte[], byte[]>(newName, record.partition(), record.key(), record.value()));
  }
}
