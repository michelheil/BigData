package org.michael.big.data.kafka.java;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

public class DeleteConsumerGroups {
  public static void main(String[] args) {
    System.out.println("*** Starting AdminClient to delete a Consumer Group ***");

    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

    AdminClient adminClient = AdminClient.create(properties);
    String consumerGroupToBeDeleted = "console-consumer-65092";
    DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient.deleteConsumerGroups(Arrays.asList(consumerGroupToBeDeleted));

    KafkaFuture<Void> resultFuture = deleteConsumerGroupsResult.all();
    try {
      resultFuture.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    adminClient.close();
  }
}