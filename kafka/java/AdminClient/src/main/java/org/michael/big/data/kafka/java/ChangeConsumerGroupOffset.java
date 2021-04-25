package org.michael.big.data.kafka.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

public class ChangeConsumerGroupOffset {
  public static void main(String[] args) {

    String brokers = "localhost:9092";
    String consumerGroupName = "test1337";
    TopicPartition topicPartition = new TopicPartition("test", 0);
    Long offset = 4L;

    Map<TopicPartition, OffsetAndMetadata> toOffset = new HashMap<>();
    toOffset.put(topicPartition, new OffsetAndMetadata(offset));

    // Create AdminClient
    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    AdminClient adminClient = AdminClient.create(properties);

    try {
      // Check offsets before altering
      KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> offsetsBeforeResetFuture = adminClient.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata();
      System.out.println("Before: " + offsetsBeforeResetFuture.get().toString());

      // Alter offsets
      adminClient.alterConsumerGroupOffsets(consumerGroupName, toOffset).partitionResult(topicPartition).get();

      // Check offsets after altering
      KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> offsetsAfterResetFuture = adminClient.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata();
      System.out.println("After:  " + offsetsAfterResetFuture.get().toString());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } finally {
      adminClient.close();
    }

  }
}