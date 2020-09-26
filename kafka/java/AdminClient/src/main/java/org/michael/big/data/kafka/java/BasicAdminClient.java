package org.michael.big.data.kafka.java;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

public class BasicAdminClient {
    public static void main(String[] args) {
        System.out.println("*** Starting Basic AdminClient ***");

        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

        AdminClient adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic("events-test", 1, (short) 1);
        CreateTopicsResult topicResult = adminClient.createTopics(Arrays.asList(newTopic));
        KafkaFuture<Void> resultFuture = topicResult.all();
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