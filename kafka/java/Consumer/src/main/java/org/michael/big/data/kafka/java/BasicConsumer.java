/*
package org.michael.big.data.kafka.java;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {
    public static void main(String[] args) {
        System.out.println("*** Starting Basic Consumer ***");

        // define Topic to which records are produced to
        final String topic = "hello-world-topic";

        // Define Kafka Parameters
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        settings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // consume messages with `poll` call and print out results
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(settings)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    String headersString = new String(record.headers().lastHeader("type").value());
                    System.out.printf("Header value for key type = %s\n", headersString);
                }
            }
        }
    }
}
*/