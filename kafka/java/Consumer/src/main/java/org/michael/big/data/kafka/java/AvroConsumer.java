package org.michael.big.data.kafka.java;

import java.util.Collections;
import java.util.Properties;

import com.kafkainaction.Alert;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroConsumer {
    public static void main(String[] args) {
        System.out.println("*** Starting Basic Consumer ***");

        // define Topic to which records are produced to
        final String topic = "avrotest";

        // Define Kafka Parameters
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer2");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //settings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        settings.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaConsumer<String, Alert> consumer = new KafkaConsumer<>(settings);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, Alert> records = consumer.poll(100);
            for (ConsumerRecord<String, Alert> record : records) {
                String key = record.key();
                Alert value = record.value();
                System.out.printf("offset = %d, key = %s, SensorID = %d, Time = %d, Status = %s\n",
                        record.offset(), key, value.getSensorId(), value.getTime(), value.getStatus());
            }
        }
    }
}
