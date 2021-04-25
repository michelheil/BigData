package org.michael.big.data.kafka.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

public class PurgeKafkaTopic {
  public static void main(String[] args) {

    String brokers = "localhost:9092";
    String topicName = "test";
    TopicPartition topicPartition = new TopicPartition(topicName, 0);
    RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(5L);

    Map<TopicPartition, RecordsToDelete> topicPartitionRecordToDelete = new HashMap<>();
    topicPartitionRecordToDelete.put(topicPartition, recordsToDelete);

    // Create AdminClient
    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    AdminClient adminClient = AdminClient.create(properties);

    try {
      adminClient.deleteRecords(topicPartitionRecordToDelete).all().get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } finally {
      adminClient.close();
    }

  }
}