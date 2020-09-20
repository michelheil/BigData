package org.michael.big.data.kafka.java;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CommitSyncTwiceConsumer {
  public static void main(String[] args) {
    System.out.println("*** Starting Basic Consumer ***");

    // define Topic to which records are produced to
    final String topic = "myTopic";

    // Define Kafka Parameters
    Properties settings = new Properties();
    settings.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer");
    settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    settings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    TopicPartition zeroTopicPartition = new TopicPartition("myOtherTopic", 0);
    OffsetAndMetadata offsetOfOtherTopic = new OffsetAndMetadata(3L);

    Map<TopicPartition, OffsetAndMetadata> commitOffsetOfOtherTopic = new HashMap<>();
    commitOffsetOfOtherTopic.put(zeroTopicPartition, offsetOfOtherTopic);


    // consume messages with `poll` call and print out results
    try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(settings)) {
      consumer.subscribe(Arrays.asList(topic));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
          consumer.commitSync();
        }
        consumer.commitSync(commitOffsetOfOtherTopic);
      }
    }
  }
}