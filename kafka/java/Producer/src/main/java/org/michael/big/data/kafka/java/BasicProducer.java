package org.michael.big.data.kafka.java;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicProducer {

    public static void main(String[] args) {
        System.out.println("*** Starting Basic Producer ***");

        // define Topic to which records are produced to
        final String topic = "hello-world-topic";

        // Define kafkaParameter
        Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer");
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // create KafkaProducer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(settings);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping Basic Producer ###");
            producer.close();
        }));

        // produce messages through `send` call
        for(int i=1; i<=5; i++) {
            final String key = "key-" + i;
            final String value = "value-" + i;
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            record.headers().add(new RecordHeader("type", "record_created".getBytes()));
            producer.send(record);
        }
    }
}